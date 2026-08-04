import { describe, expect, test } from "bun:test";
import { Effect, Stream } from "effect";
import { ProviderRequestKind, RuntimeMessageRole, SystemCacheHint, SystemSegmentKind } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { MaxTextBytes } from "@tetral/gateway-protocol/src/bounds.js";
import type { SessionEvent, SessionEventWriter, SessionEventWriterRequestEndEnvelope } from "../../../src/contracts/runtime.js";
import { RuntimeMessageSchema } from "../../../src/contracts/runtime.js";
import type { LLMEvent } from "../../../src/llm/llm-event.js";
import { runtimeFailureFromProviderError } from "../../../src/llm/llm-event.js";
import type { Interface as LLMServiceInterface, LLMRequest } from "../../../src/llm/llm-service.js";
import {
  ApplyPatchInstructionsText,
  PlatformBaseSystemPrompt,
  assembleProviderCallRequest,
  renderSkillGuidanceSegment,
} from "../../../src/thread-loop/provider-request.js";
import type {
  MemoryStorePromptEntry,
  ProviderCallAssemblyInput,
  SkillGuidanceIndexEntry,
} from "../../../src/thread-loop/provider-request.js";
import * as ThreadLoop from "../../../src/thread-loop/thread-loop.js";
import { normalizeProviderError } from "../../../src/contracts/provider.js";
import { ThreadRuntime } from "../../../src/thread-loop/thread-runtime.js";
import type { RuntimeConfigPatchState } from "../../../src/thread-loop/thread-state.js";
import { runtimeModelForThread, runtimeToolPolicyFromPatchPayloads } from "../../../../runtime-pod/src/command.js";
import { buildThreadLoopUserMessage as userMessage, buildThreadLoopDurableRuntimeNotificationMessage as runtimeNotificationMessage } from "../runtime-message-builders.js";
import { acceptedInputReceipt } from "../runtime-declaration-fixtures.js";
import { QueuedContextLoader, RecordingContextLoader, RecordingRuntimeMetrics, ThreadLoopRuntimeStore, acceptedInput, approvalReviewerPolicy, catalogForTest, compactionTransportHistory, createdAt, failingEventWriter, llmService, queuedLLMService, runtimeThreadLoopLayer, testRunCustody, utf8RoundTrip, writerFrom } from "./thread-loop-test-support.js";
import type { TestContextLoader } from "./thread-loop-test-support.js";

describe("ThreadLoop", () => {
test("reports declaration, event-write, and provider stream metrics through injected sink", async () => {
    const metrics = new RecordingRuntimeMetrics();
    const loader = new RecordingContextLoader([], {
        type: "messages",
        messages: [userMessage("msg_user_1", 1, "hello")],
    });
    const result = await Effect.runPromise(Effect.gen(function* () {
        const threadLoop = yield* ThreadLoop.Service;
        return yield* threadLoop.run(new ThreadRuntime("sesn_1"), testRunCustody());
    }).pipe(Effect.provide(runtimeThreadLoopLayer(loader, { metrics }))));
    expect(result.type).toBe("completed");
    expect(metrics.contextLoadLatencies).toContainEqual(expect.objectContaining({
        operation: "commit_accepted_input",
        outcome: "success",
    }));
    expect(metrics.eventWriteLatencies).toContainEqual(expect.objectContaining({
        operation: "append",
        outcome: "success",
    }));
    expect(metrics.eventWriteLatencies).toContainEqual(expect.objectContaining({
        operation: "finish_idle",
        outcome: "success",
    }));
    expect(metrics.providerStreamDurations).toContainEqual(expect.objectContaining({
        kind: "agent_provider_request",
        outcome: "success",
    }));
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
        runtimeMessages: [
            {
                id: "message_user_1",
                role: RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_USER,
                status: "completed",
                origin: "user",
                parts: [{ id: "part_user_1", text: { text: "hello" } }],
            },
        ],
        runtime: {
            systemInstructions: "third group runtime system instructions",
            agentSystem: "Operate as the session specialist.",
            toolCatalog: catalogForTest({
                name: "third_group_lookup",
                description: "third group tool description",
                inputSchema: { type: "object", properties: { q: { type: "string" } } },
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
                inputSchemaJson: "{\"type\":\"object\",\"properties\":{\"q\":{\"type\":\"string\"}}}",
            },
        ],
        maxOutputTokens: 321,
        timeoutMs: 456,
        request: {
            requestId: "provider_request_1",
            modelRequestId: "model_request_1",
            requestKind: ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
            workspaceId: "workspace_1",
            sessionId: "sesn_1",
            sessionThreadId: "thread_1",
            parentThreadId: "parent_thread_1",
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
            messages: [
                {
                    id: "message_user_1",
                    role: RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_USER,
                    status: "completed",
                    origin: "user",
                    parts: [{ id: "part_user_1", text: { text: "hello" } }],
                },
            ],
            tools: [
                {
                    name: "third_group_lookup",
                    description: "third group tool description",
                    inputSchemaJson: "{\"type\":\"object\",\"properties\":{\"q\":{\"type\":\"string\"}}}",
                },
            ],
            attachments: [],
            limits: { maxOutputTokens: 321, timeoutMs: 456 },
        },
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
            requestKind: ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
            approvalReviewerPolicy,
            outputSchemaJson,
        },
    });
    expect(reviewer.ok ? reviewer.request.outputSchemaJson : undefined).toBe(outputSchemaJson);
    expect(assembleProviderCallRequest({
        ...input,
        runtime: {
            ...input.runtime,
            requestKind: ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
        },
    }).ok).toBe(false);
    expect(assembleProviderCallRequest({
        ...input,
        runtime: { ...input.runtime, outputSchemaJson },
    }).ok).toBe(false);
});
test("Bridge-shaped create-time config installs agent and memory system segments on provider snapshots", async () => {
    const coldPayload = JSON.stringify({
        config_generation: 7,
        runtime_config: {
            installedTools: [{ type: "tetral_agent_toolset", family: "claude" }],
            system: "Operate as the session specialist.",
            memoryStores: [{
                    memoryStoreId: "memstore_notes",
                    name: "Project notes",
                    access: "read_write",
                    instructions: "Preserve this guidance.",
                }],
        },
    });
    const cases = [
        {
            name: "cold bootstrap",
            patches: [coldPayload],
            expectedAgentSegments: [{
                    kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_AGENT,
                    text: "Operate as the session specialist.",
                    cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_SESSION,
                }],
            expectedMemorySegments: [{
                    kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_MEMORY,
                    text: "Memory store: Project notes\nAccess: read_write\nInstructions:\nPreserve this guidance.",
                    cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_SESSION,
                }],
        },
        {
            name: "create-time nullable fields",
            patches: [JSON.stringify({
                    config_generation: 7,
                    runtime_config: {
                        installedTools: [{ type: "tetral_agent_toolset", family: "claude" }],
                        system: null,
                        memoryStores: [{
                                memoryStoreId: "memstore_reference",
                                name: "Reference",
                                access: "read_only",
                                instructions: null,
                            }],
                    },
                })],
            expectedAgentSegments: [],
            expectedMemorySegments: [{
                    kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_MEMORY,
                    text: "Memory store: Reference\nAccess: read_only",
                    cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_SESSION,
                }],
        },
    ] as const;
    for (const scenario of cases) {
        const session = new ThreadRuntime(`sesn_agent_system_${scenario.name.replaceAll(" ", "_")}`);
        const requests: LLMRequest[] = [];
        const loader = new RecordingContextLoader([], {
            type: "messages",
            messages: [userMessage(`user-agent-system-${scenario.name}`, 0, "hello")],
        });
        const result = await Effect.runPromise(Effect.gen(function* () {
            const threadLoop = yield* ThreadLoop.Service;
            return yield* threadLoop.run(session, testRunCustody());
        }).pipe(Effect.provide(runtimeThreadLoopLayer(loader, {
            onStream: (request) => requests.push(request),
            runtimePolicy: () => runtimeToolPolicyFromPatchPayloads(scenario.patches),
        }))));
        expect(result).toMatchObject({ type: "completed" });
        expect(requests).toHaveLength(1);
        expect(requests[0]?.system.filter((segment) => segment.kind === SystemSegmentKind.SYSTEM_SEGMENT_KIND_AGENT)).toEqual([...scenario.expectedAgentSegments]);
        expect(requests[0]?.system.filter((segment) => segment.kind === SystemSegmentKind.SYSTEM_SEGMENT_KIND_MEMORY)).toEqual([...scenario.expectedMemorySegments]);
    }
});
test("provider snapshot injects apply-patch instructions from the cold pinned GPT family", async () => {
    const session = new ThreadRuntime("sesn_gpt_patch_prompt");
    const payloadJson = JSON.stringify({
        config_generation: 1,
        runtime_config: {
            installedTools: [{ type: "tetral_agent_toolset", family: "gpt" }],
            system: null,
            memoryStores: [],
        },
    });
    expect(session.configuration.apply({
        generation: 1,
        payloadJson,
        coldLoad: true,
        installedBuiltinFamily: "gpt",
    })).toBe("applied");
    const requests: LLMRequest[] = [];
    const loader = new RecordingContextLoader([], {
        type: "messages",
        messages: [userMessage("user-gpt-patch-prompt", 0, "edit a file")],
    });
    const result = await Effect.runPromise(Effect.gen(function* () {
        const threadLoop = yield* ThreadLoop.Service;
        return yield* threadLoop.run(session, testRunCustody());
    }).pipe(Effect.provide(runtimeThreadLoopLayer(loader, {
        onStream: (request) => requests.push(request),
        runtimePolicy: () => runtimeToolPolicyFromPatchPayloads([payloadJson]),
    }))));
    expect(result).toMatchObject({ type: "completed" });
    expect(requests[0]?.system[0]?.text).toContain("## `apply_patch`");
    expect(requests[0]?.system[0]?.text).toContain("do not JSON-wrap it");
});
test("first accepted turn resolves its provider model from the cold runtime config", async () => {
    const session = new ThreadRuntime("sesn_first_config_model");
    session.state.enqueueAcceptedInput(acceptedInput("rin_first_config_model", session.sessionId));
    const runtimeConfigPatch: RuntimeConfigPatchState = {
        requestId: "req_first_config_patch",
        workspaceId: "wksp_test",
        sessionId: session.sessionId,
        sessionThreadId: "thrd_1",
        bindingId: "bind_1",
        bindingGeneration: 1,
        targetPodUid: "pod_1",
        runtimeInputId: "rin_first_config_patch",
        eventIds: [],
        sequenceFrom: 0,
        sequenceTo: 0,
        generation: 1,
        coldLoad: true,
        installedBuiltinFamily: "claude" as const,
        payloadJson: JSON.stringify({
            runtime_config: {
                agent: { config: { model: "openai/gpt-5.5" } },
                installedTools: [{ type: "tetral_agent_toolset", family: "claude" }],
            },
        }),
    };
    session.configuration.apply(runtimeConfigPatch);
    const loader = new QueuedContextLoader([], []);
    const requests: LLMRequest[] = [];
    const result = await Effect.runPromise(Effect.gen(function* () {
        return yield* (yield* ThreadLoop.Service).run(session, testRunCustody());
    }).pipe(Effect.provide(runtimeThreadLoopLayer(loader, {
        runtimeModel: (activeSession) => runtimeModelForThread(activeSession.identity.threadRole, activeSession.configuration.patches().map((patch) => patch.payloadJson), { providerId: "anthropic", modelId: "claude-opus-4-8" }),
        onStream: (request) => requests.push(request),
    }))));
    expect(result).toMatchObject({ type: "completed" });
    expect(requests).toHaveLength(1);
    expect(requests[0]?.model).toEqual({ providerId: "openai", modelId: "gpt-5.5", variant: "" });
});
test("a bounded live rejection is authored by the loop and committed before provider work", async () => {
    const session = new ThreadRuntime("sesn_live_rejection");
    const firstInput = {
        ...acceptedInput("rin_live_rejection", session.sessionId),
        kind: "rejection" as const,
        reasonCode: "runtime_command_payload_too_large" as const,
    };
    const secondInput = {
        ...acceptedInput("rin_live_rejection_second", session.sessionId),
        kind: "rejection" as const,
        reasonCode: "runtime_command_payload_too_large" as const,
    };
    session.state.enqueueAcceptedInput(firstInput);
    session.state.enqueueAcceptedInput(secondInput);
    const submittedDrafts: Array<readonly unknown[]> = [];
    const loader: TestContextLoader = {
        buildContext: async () => [],
        loadPendingInput: async () => ({ type: "empty" }),
        commitAcceptedInput: async (accepted, options) => {
            submittedDrafts.push([...(options?.drafts ?? [])]);
            return acceptedInputReceipt(accepted);
        },
    };
    let providerCalls = 0;
    const result = await Effect.runPromise(Effect.gen(function* () {
        return yield* (yield* ThreadLoop.Service).run(session, testRunCustody());
    }).pipe(Effect.provide(runtimeThreadLoopLayer(loader, {
        onStream: () => {
            providerCalls += 1;
        },
    }))));
    expect(result).toMatchObject({ type: "completed" });
    expect(submittedDrafts).toHaveLength(2);
    for (const drafts of submittedDrafts) {
        expect(drafts).toEqual([
            expect.objectContaining({
                role: "assistant",
                origin: "agent",
                status: "completed",
                parts: [expect.objectContaining({
                        type: "text",
                        text: "The session runtime could not accept this input.",
                    })],
            }),
        ]);
    }
    expect(session.state.contextManager.messages()).toContainEqual(expect.objectContaining({
        role: "assistant",
        parts: [expect.objectContaining({
                type: "text",
                text: "The session runtime could not accept this input.",
            })],
    }));
    expect(session.state.contextManager.messages().filter((message) =>
        message.parts.some((part) =>
            part.type === "text" && part.text === "The session runtime could not accept this input."
        )
    )).toHaveLength(2);
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
    session.state.enqueueAcceptedInput(acceptedInput("rin_stale_request_start", session.sessionId));
    const baseWriter = writerFrom((envelope) => ({
        ok: true,
        writeId: envelope.writeId,
        eventId: `bridge-${envelope.writeId}`,
        processedAt: createdAt,
    }));
    let providerCalls = 0;
    const result = await Effect.runPromise(Effect.gen(function* () {
        return yield* (yield* ThreadLoop.Service).run(session, testRunCustody());
    }).pipe(Effect.provide(runtimeThreadLoopLayer(new QueuedContextLoader([], []), {
        writer: {
            ...baseWriter,
            append: async (envelope) => {
                const written = await baseWriter.append(envelope);
                if (envelope.event.type !== "span.model_request_start" || !written.ok || written.declaration === undefined) {
                    return written;
                }
                return {
                    ...written,
                    declaration: { ...written.declaration, applicationDisposition: "stale_custody" as const },
                };
            },
        },
        onStream: () => {
            providerCalls += 1;
        },
    }))));
    expect(result).toEqual({ type: "interrupted", discardHotState: true });
    expect(providerCalls).toBe(0);
    expect(session.state.contextManager.messages()).toEqual([]);
});
test("runtime layer emits running, span, progress, span end, and idle around a normal provider call", async () => {
    const session = new ThreadRuntime("sesn_1");
    const store = new ThreadLoopRuntimeStore([]);
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const timeline: string[] = [];
    const appended: SessionEvent[] = [];
    const writer = writerFrom((envelope) => {
        timeline.push(`event:${envelope.event.type}`);
        appended.push(envelope.event);
        return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
    });
    const result = await Effect.runPromise(Effect.gen(function* () {
        const threadLoop = yield* ThreadLoop.Service;
        return yield* threadLoop.run(session, testRunCustody());
    }).pipe(Effect.provide(runtimeThreadLoopLayer(loader, {
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
    }))));
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
        model_request_start_id: expect.stringMatching(/^bridge-event_write-/),
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
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const appendedTypes: string[] = [];
    const completeWriter = writerFrom((envelope) => {
        appendedTypes.push(envelope.event.type);
        return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
    });
    const malformedWriter = {
        append: completeWriter.append,
        finishIdle: completeWriter.finishIdle,
    } as unknown as SessionEventWriter;
    const providerRequests: LLMRequest[] = [];
    const result = await Effect.runPromise(Effect.gen(function* () {
        const threadLoop = yield* ThreadLoop.Service;
        return yield* threadLoop.run(session, testRunCustody());
    }).pipe(Effect.provide(runtimeThreadLoopLayer(loader, {
        writer: malformedWriter,
        onStream: (request) => providerRequests.push(request),
        events: [
            { type: "text-start", id: "text-1" },
            { type: "text-delta", id: "text-1", text_delta: "hello" },
            { type: "text-end", id: "text-1" },
            { type: "finish", finishReason: "stop" },
        ],
    }))));
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
    session.state.updateCurrentModel({ providerId: "fake", modelId: "fake-chat" });
    session.state.contextManager.installThreadContextPrefix({
        childThreadId: "thrd_child",
        parentThreadId: "thrd_parent",
        parentBoundaryEventId: "sevt_parent_boundary",
        entries: [userMessage("parent-prefix", 41, "PARENT_PREFIX_SENTINEL")],
        createdAt,
    });
    session.state.recordLastRequestCompletion({
        inputTokens: 96000,
        outputTokens: 75,
        reasoningTokens: 0,
        cacheReadTokens: 0,
        cacheWriteTokens: 0,
    }, {
        contextWindowTokens: 100000,
        outputTokenLimit: 4096,
    }, 0);
    const mediaOnlyProjection = RuntimeMessageSchema.parse({
        ...userMessage("user-media-only", 0, "discarded media placeholder"),
        parts: [],
    });
    const loader = new RecordingContextLoader([
        mediaOnlyProjection,
        userMessage("user-old", 1, compactionTransportHistory("old context that should be summarized")),
    ], { type: "messages", messages: [userMessage("user-new", 2, "new request")] });
    const compactionBoundaryOrder: string[] = [];
    const requests: LLMRequest[] = [];
    const oversizedSummary = `Summary carried forward.${"S".repeat(40000)}`;
    const queuedLlm = queuedLLMService([
        [
            { type: "text-start", id: "summary-text" },
            { type: "text-delta", id: "summary-text", text_delta: oversizedSummary },
            { type: "text-end", id: "summary-text" },
            {
                type: "finish",
                finishReason: "stop",
                usage: { inputTokens: 20, outputTokens: 5, reasoningTokens: 0, cacheReadTokens: 0, cacheWriteTokens: 0 },
            },
        ],
        [
            { type: "text-start", id: "answer-text" },
            { type: "text-delta", id: "answer-text", text_delta: "answer after compaction" },
            { type: "text-end", id: "answer-text" },
            {
                type: "finish",
                finishReason: "stop",
                usage: { inputTokens: 9, outputTokens: 4, reasoningTokens: 0, cacheReadTokens: 0, cacheWriteTokens: 0 },
                modelLimits: { contextWindowTokens: 320, outputTokenLimit: 120 },
            },
        ],
    ], requests);
    const llm: LLMServiceInterface = {
        stream(request) {
            if (request.requestKind === ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST) {
                compactionBoundaryOrder.push("normal-provider-stream-start");
            }
            return queuedLlm.stream(request);
        },
    };
    const appended: SessionEvent[] = [];
    const requestEndEnvelopes: SessionEventWriterRequestEndEnvelope[] = [];
    const baseWriter = writerFrom((envelope) => {
        appended.push(envelope.event);
        if (envelope.event.type === "span.model_request_start" && !compactionBoundaryOrder.includes("compaction-start-ack")) {
            compactionBoundaryOrder.push("compaction-start-ack");
        }
        return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
    });
    const writer: SessionEventWriter = {
        ...baseWriter,
        writeRequestEnd: async (envelope) => {
            requestEndEnvelopes.push(envelope);
            if (envelope.requestKind === "compaction_summary" && !envelope.isError) {
                compactionBoundaryOrder.push("compaction-request-end-and-event-ack");
            }
            return await baseWriter.writeRequestEnd!(envelope);
        },
    };
    const result = await Effect.runPromise(Effect.gen(function* () {
        const threadLoop = yield* ThreadLoop.Service;
        return yield* threadLoop.run(session, testRunCustody());
    }).pipe(Effect.provide(runtimeThreadLoopLayer(loader, {
        llmService: llm,
        writer,
        compaction: { timeoutMs: 765432 },
        providerCallRuntime: {
            systemInstructions: "normal provider system",
            maxOutputTokens: 2048,
            timeoutMs: 654321,
            toolCatalog: catalogForTest({ name: "search", description: "Search test tool", inputSchema: { type: "object", properties: {} } }),
        },
    }))));
    expect(result).toMatchObject({ type: "completed", modelMessageCount: 1 });
    expect(requests).toHaveLength(2);
    expect(requests[0]?.requestKind).toBe(ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY);
    expect(requests[0]?.model).toEqual({ providerId: "fake", modelId: "fake-chat", variant: "" });
    expect(requests[0]?.system).toEqual([]);
    expect(requests[0]?.limits?.maxOutputTokens).toBe(2048);
    expect(requests[0]?.limits?.timeoutMs).toBe(765432);
    expect(requests[0]?.model?.variant).toBe("");
    expect(requests[0]?.tools).toEqual([]);
    const compactionPromptParts = requests[0]?.messages[0]?.parts.flatMap((part) => part.text?.text ?? []) ?? [];
    expect(compactionPromptParts).toHaveLength(1);
    const compactionPrompt = compactionPromptParts[0] ?? "";
    expect(new TextEncoder().encode(compactionPrompt).byteLength).toBeLessThanOrEqual(64 * 1024);
    expect(compactionPrompt).toStartWith("Create a new anchored summary from the conversation history.");
    expect(compactionPrompt).toContain("## Objective");
    expect(compactionPrompt).toContain("[User]:\n\n[User]: old context that should be summarized");
    expect(compactionPrompt).toContain("[User]: old context that should be summarized");
    expect(compactionPrompt).toContain("PARENT_PREFIX_SENTINEL");
    expect(compactionPrompt).toContain("😀");
    expect(utf8RoundTrip(compactionPrompt)).toBe(compactionPrompt);
    expect(compactionPrompt).not.toContain("<previous-summary>");
    expect(compactionPrompt).not.toContain("RECENT_SENTINEL");
    expect(requests[1]?.requestKind).toBe(ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST);
    expect(requests[1]?.limits?.timeoutMs).toBe(654321);
    expect(requests[1]?.model).toEqual({ providerId: "fake", modelId: "fake-chat", variant: "" });
    expect(requests[1]?.model?.variant).toBe("");
    expect(requests[1]?.tools.map((tool) => tool.name)).toEqual(["search"]);
    expect(requests[1]?.messages).toHaveLength(1);
    expect(JSON.stringify(requests[1]?.messages[0])).toContain("<conversation-checkpoint>");
    expect(JSON.stringify(requests[1]?.messages[0])).toContain("Summary carried forward.");
    expect(appended.map((event) => event.type)).toEqual([
        "session.status_running",
        "span.model_request_start",
        "span.model_request_end",
        "span.model_request_start",
        "agent.message",
        "span.model_request_end",
        "session.status_idle",
    ]);
    expect(requestEndEnvelopes.map((envelope) => envelope.requestKind)).toEqual(["compaction_summary", undefined]);
    expect(requestEndEnvelopes[0]?.prefixConsumption).toEqual({
        childThreadId: "thrd_child",
        parentBoundaryEventId: "sevt_parent_boundary",
        checkpointRuntimeLocalId: expect.any(String),
    });
    expect(compactionBoundaryOrder).toEqual([
        "compaction-start-ack",
        "compaction-request-end-and-event-ack",
        "normal-provider-stream-start",
    ]);
    const hotCheckpoint = session.state.contextManager.messages().find((message) => message.origin === "runtime");
    expect(hotCheckpoint).toBeDefined();
    expect(hotCheckpoint?.updatedAt).toBe(hotCheckpoint?.createdAt);
    expect(hotCheckpoint?.parts[0]).toMatchObject({ type: "text", status: "completed" });
    expect(hotCheckpoint?.parts[0]?.updatedAt).toBe(hotCheckpoint?.parts[0]?.createdAt);
    const checkpointText = hotCheckpoint?.parts.flatMap((part) => part.type === "text" ? [part.text] : []).join("") ?? "";
    expect(new TextEncoder().encode(checkpointText).byteLength).toBeLessThanOrEqual(60 * 1024);
    expect(checkpointText).toContain("<summary>\nSummary carried forward.");
    expect(checkpointText).toContain("RECENT_SENTINEL");
    expect(checkpointText).toContain("[User]: new request\n</recent-context>");
    expect(utf8RoundTrip(checkpointText)).toBe(checkpointText);
    expect(checkpointText.indexOf("RECENT_SENTINEL")).toBeLessThan(checkpointText.indexOf("[User]: new request"));
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
        type: "messages",
        messages: [userMessage("user-1", 0, "retry this request")],
    });
    const requests: LLMRequest[] = [];
    const failedReasoning = "failed attempt private reasoning";
    const failedDraft = "failed attempt draft";
    const successfulReasoningFirst = "successful first reasoning part";
    const successfulReasoningSecond = "successful second reasoning part";
    const successfulReasoning = [successfulReasoningFirst, successfulReasoningSecond];
    const llm = queuedLLMService([
        [
            { type: "reasoning-start", id: "retry-discarded-reasoning" },
            { type: "reasoning-delta", id: "retry-discarded-reasoning", text_delta: failedReasoning },
            { type: "reasoning-end", id: "retry-discarded-reasoning" },
            { type: "text-start", id: "retry-discarded-text" },
            { type: "text-delta", id: "retry-discarded-text", text_delta: failedDraft },
            {
                type: "provider-error",
                error: runtimeFailureFromProviderError(normalizeProviderError({
                    code: "provider_unavailable",
                    message: "temporary provider failure",
                    retryable: true,
                    fatal: false,
                })),
            },
        ],
        [
            { type: "reasoning-start", id: "retry-success-reasoning-1" },
            { type: "reasoning-delta", id: "retry-success-reasoning-1", text_delta: successfulReasoningFirst },
            { type: "reasoning-end", id: "retry-success-reasoning-1" },
            { type: "reasoning-start", id: "retry-success-reasoning-2" },
            { type: "reasoning-delta", id: "retry-success-reasoning-2", text_delta: successfulReasoningSecond },
            { type: "reasoning-end", id: "retry-success-reasoning-2" },
            { type: "text-start", id: "answer-text" },
            { type: "text-delta", id: "answer-text", text_delta: "recovered" },
            { type: "text-end", id: "answer-text" },
            { type: "finish", finishReason: "stop" },
        ],
    ], requests);
    const appended: SessionEvent[] = [];
    const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
    const baseWriter = writerFrom((envelope) => {
        appended.push(envelope.event);
        return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
    });
    const writer: SessionEventWriter = {
        ...baseWriter,
        writeRequestEnd: async (envelope) => {
            requestEnds.push(envelope);
            return await baseWriter.writeRequestEnd(envelope);
        },
    };
    const result = await Effect.runPromise(Effect.gen(function* () {
        const threadLoop = yield* ThreadLoop.Service;
        return yield* threadLoop.run(session, testRunCustody());
    }).pipe(Effect.provide(runtimeThreadLoopLayer(loader, {
        llmService: llm,
        writer,
        runtimePolicy: () => ({ providerRescheduleBudget: 3, compactionRescheduleBudget: 2 }),
    }))));
    expect(result).toMatchObject({ type: "completed", modelMessageCount: 1 });
    expect(requests).toHaveLength(2);
    expect(requests.map((request) => request.attachments)).toEqual([
        [pendingFileAttachment],
        [pendingFileAttachment],
    ]);
    expect(JSON.stringify(requests[1]?.messages)).toContain("retry this request");
    expect(JSON.stringify(requests[1]?.messages)).not.toContain("temporary provider failure");
    expect(JSON.stringify(requests[1]?.messages)).not.toContain(failedReasoning);
    expect(JSON.stringify(requests[1]?.messages)).not.toContain(failedDraft);
    expect(requestEnds).toHaveLength(2);
    expect(requestEnds[0]?.reschedule).toMatchObject({ attempt: 1 });
    expect(requestEnds[0]?.consumedFileAttachments ?? []).toEqual([]);
    expect(requestEnds[0]?.stableReasoningParts).toBeUndefined();
    expect(requestEnds[1]?.reschedule).toBeUndefined();
    expect(requestEnds[1]?.consumedFileAttachments).toEqual([{
            sourceEventId: "sevt_retry_file",
            fileId: "file_retry",
        }]);
    expect(requestEnds[1]?.stableReasoningParts?.map((part) => part.text)).toEqual(successfulReasoning);
    expect(requestEnds.filter((envelope) => (envelope.stableReasoningParts?.length ?? 0) > 0)).toHaveLength(1);
    expect(new Set(requestEnds.map((envelope) => envelope.modelRequestId)).size).toBe(2);
    const durableEvents = JSON.stringify(appended);
    const hotContext = JSON.stringify(session.state.contextManager.messages());
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
            error: expect.objectContaining({ retryStatus: { type: "retrying", attempt: 1 } }),
        }),
    ]);
    expect(appended.filter((event) => event.type === "session.status_idle")).toHaveLength(1);
});
test("deterministic Gateway rejection closes on the first attempt without rescheduling", async () => {
    const session = new ThreadRuntime("sesn_gateway_protocol_rejection");
    const loader = new RecordingContextLoader([], {
        type: "messages",
        messages: [userMessage("user-gateway-protocol", 0, "send this request")],
    });
    const requests: LLMRequest[] = [];
    const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
    const baseWriter = writerFrom((envelope) => ({
        ok: true,
        writeId: envelope.writeId,
        eventId: `bridge-${envelope.writeId}`,
        processedAt: createdAt,
    }));
    const writer: SessionEventWriter = {
        ...baseWriter,
        writeRequestEnd: async (envelope) => {
            requestEnds.push(envelope);
            return await baseWriter.writeRequestEnd(envelope);
        },
    };
    const result = await Effect.runPromise(Effect.gen(function* () {
        const threadLoop = yield* ThreadLoop.Service;
        return yield* threadLoop.run(session, testRunCustody());
    }).pipe(Effect.provide(runtimeThreadLoopLayer(loader, {
        writer,
        llmService: queuedLLMService([[
                {
                    type: "provider-error",
                    error: {
                        type: "runtime",
                        code: "gateway_protocol_error",
                        message: "Gateway rejected the provider request.",
                        retryable: false,
                        fatal: true,
                    },
                },
            ]], requests),
        runtimePolicy: () => ({ providerRescheduleBudget: 3, compactionRescheduleBudget: 2 }),
    }))));
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
test("a stale no-content request-end receipt discards hot state before another provider request", async () => {
    const session = new ThreadRuntime("sesn_stale_empty_request_end");
    const loader = new RecordingContextLoader([], {
        type: "messages",
        messages: [userMessage("user-stale-empty-end", 0, "send this request")],
    });
    const requests: LLMRequest[] = [];
    const baseWriter = writerFrom((envelope) => ({
        ok: true,
        writeId: envelope.writeId,
        eventId: `bridge-${envelope.writeId}`,
        processedAt: createdAt,
    }));
    const writer: SessionEventWriter = {
        ...baseWriter,
        writeRequestEnd: async (envelope) => {
            const result = await baseWriter.writeRequestEnd(envelope);
            if (!result.ok || result.declaration === undefined) {
                return result;
            }
            return {
                ...result,
                declaration: {
                    ...result.declaration,
                    applicationDisposition: "stale_custody",
                },
            };
        },
    };
    const result = await Effect.runPromise(Effect.gen(function* () {
        const threadLoop = yield* ThreadLoop.Service;
        return yield* threadLoop.run(session, testRunCustody());
    }).pipe(Effect.provide(runtimeThreadLoopLayer(loader, {
        writer,
        llmService: queuedLLMService([[
                {
                    type: "provider-error",
                    error: runtimeFailureFromProviderError(normalizeProviderError({
                        code: "provider_unavailable",
                        message: "retryable provider failure",
                        retryable: true,
                        fatal: false,
                    })),
                },
            ]], requests),
        runtimePolicy: () => ({ providerRescheduleBudget: 3, compactionRescheduleBudget: 2 }),
    }))));
    expect(result).toEqual({ type: "interrupted", discardHotState: true });
    expect(requests).toHaveLength(1);
});
test("runtime layer requests hot-state discard when running status append fails before provider work", async () => {
    const order: string[] = [];
    const session = new ThreadRuntime("sesn_1");
    const store = new ThreadLoopRuntimeStore(order);
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const appendedTypes: string[] = [];
    let providerCalled = false;
    const result = await Effect.runPromise(Effect.gen(function* () {
        const threadLoop = yield* ThreadLoop.Service;
        return yield* threadLoop.run(session, testRunCustody());
    }).pipe(Effect.provide(runtimeThreadLoopLayer(loader, {
        store,
        writer: failingEventWriter(appendedTypes, (event) => event.type === "session.status_running"),
        onStream: () => {
            providerCalled = true;
        },
    }))));
    expect(result).toMatchObject({
        type: "failed",
        error: { type: "session-event-writer", code: "unavailable" },
        releaseSession: { reason: "event_write_failed" },
    });
    expect(providerCalled).toBe(false);
    expect(appendedTypes).toEqual(["session.status_running"]);
    expect(order).toEqual([]);
    expect(session.state.contextManager.messages()).toEqual([]);
});
test("provider-call assembly failure fails closed after running status but before assistant shell, span, and provider stream", async () => {
    const order: string[] = [];
    const session = new ThreadRuntime("sesn_1");
    const store = new ThreadLoopRuntimeStore(order);
    const appended: SessionEvent[] = [];
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const hostileMarker = "prompt text raw provider payload marker authorization: bearer dummy-thirdgroup-token";
    let providerCalled = false;
    const result = await Effect.runPromise(Effect.gen(function* () {
        const threadLoop = yield* ThreadLoop.Service;
        return yield* threadLoop.run(session, testRunCustody());
    }).pipe(Effect.provide(runtimeThreadLoopLayer(loader, {
        store,
        providerCallAssembler: () => {
            throw new Error(hostileMarker);
        },
        onStream: () => {
            providerCalled = true;
        },
        writer: writerFrom((envelope) => {
            appended.push(envelope.event);
            return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
        }),
    }))));
    expect(result).toMatchObject({
        type: "failed",
        error: { type: "runtime", code: "runtime_invalid_sequence", reason: "runtime_contract_validation" },
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
        { type: "session.status_idle", stop_reason: { type: "retries_exhausted" } },
    ]);
    expect(session.state.contextManager.messages()).toEqual([
        expect.objectContaining({
            role: "user",
            parts: [expect.objectContaining({ type: "text", text: "hello" })],
        }),
    ]);
});
test("runtime layer requests hot-state discard when span start append fails after shell persistence", async () => {
    const order: string[] = [];
    const session = new ThreadRuntime("sesn_1");
    const store = new ThreadLoopRuntimeStore(order);
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const appendedTypes: string[] = [];
    let providerCalled = false;
    const result = await Effect.runPromise(Effect.gen(function* () {
        const threadLoop = yield* ThreadLoop.Service;
        return yield* threadLoop.run(session, testRunCustody());
    }).pipe(Effect.provide(runtimeThreadLoopLayer(loader, {
        store,
        writer: failingEventWriter(appendedTypes, (event) => event.type === "span.model_request_start"),
        onStream: () => {
            providerCalled = true;
        },
    }))));
    expect(result).toMatchObject({
        type: "failed",
        error: { type: "session-event-writer", code: "unavailable" },
        releaseSession: { reason: "event_write_failed" },
    });
    expect(providerCalled).toBe(false);
    expect(appendedTypes).toEqual(["session.status_running", "span.model_request_start"]);
    expect(order).toEqual([]);
    expect(session.state.contextManager.messages().map((message) => message.role)).toEqual(["user"]);
});
test("runtime layer requests hot-state discard when span end append fails after durable progress", async () => {
    const order: string[] = [];
    const session = new ThreadRuntime("sesn_1");
    const store = new ThreadLoopRuntimeStore(order);
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const appendedTypes: string[] = [];
    const result = await Effect.runPromise(Effect.gen(function* () {
        const threadLoop = yield* ThreadLoop.Service;
        return yield* threadLoop.run(session, testRunCustody());
    }).pipe(Effect.provide(runtimeThreadLoopLayer(loader, {
        store,
        writer: failingEventWriter(appendedTypes, (event) => event.type === "span.model_request_end"),
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
    }))));
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
    expect(session.state.contextManager.messages().at(-1)?.parts).toEqual([
        expect.objectContaining({ type: "text", text: "ok", status: "completed" }),
    ]);
    expect(session.state.lastRequestUsage()).toBeUndefined();
});
test("commits completed reasoning with provider metadata only after durable request settlement", async () => {
    const session = new ThreadRuntime("sesn_1");
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const order: string[] = [];
    const requestEnds: Parameters<SessionEventWriter["writeRequestEnd"]>[0][] = [];
    const writer = writerFrom((envelope) => {
        order.push(`event:${envelope.event.type}`);
        return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
    }, async (envelope) => {
        order.push("event:span.model_request_end");
        requestEnds.push(envelope);
        return { ok: true, writeId: envelope.writeId, eventId: envelope.writeId, processedAt: createdAt };
    });
    const result = await Effect.runPromise(Effect.gen(function* () {
        const threadLoop = yield* ThreadLoop.Service;
        return yield* threadLoop.run(session, testRunCustody());
    }).pipe(Effect.provide(runtimeThreadLoopLayer(loader, {
        writer,
        events: [
            { type: "reasoning-start", id: "reasoning-1", providerMetadata: { anthropic: { signature: "sig_round_trip" } } },
            { type: "reasoning-delta", id: "reasoning-1", text_delta: "thinking" },
            { type: "reasoning-end", id: "reasoning-1" },
            { type: "reasoning-start", id: "reasoning-2", providerMetadata: { openai: { encrypted_content: "ciphertext" } } },
            { type: "reasoning-delta", id: "reasoning-2", text_delta: "again" },
            { type: "reasoning-end", id: "reasoning-2" },
            { type: "finish", finishReason: "stop" },
        ],
    }))));
    expect(result).toMatchObject({ type: "completed" });
    expect(requestEnds).toHaveLength(1);
    expect(requestEnds[0]?.stableReasoningParts).toEqual([
        expect.objectContaining({
            text: "thinking",
            providerPartId: "reasoning-1",
            providerMetadata: { anthropic: { signature: "sig_round_trip" } },
            partSequence: 0,
        }),
        expect.objectContaining({
            text: "again",
            providerPartId: "reasoning-2",
            providerMetadata: { openai: { encrypted_content: "ciphertext" } },
            partSequence: 1,
        }),
    ]);
    expect(order.filter((entry) => entry === "event:span.model_request_end")).toHaveLength(1);
});
test("keeps failed reasoning settlement out of stable hot context", async () => {
    const session = new ThreadRuntime("sesn_1");
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const writer = writerFrom((envelope) => ({ ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt }), async (envelope) => ({
        ok: false,
        error: {
            type: "session-event-writer",
            code: "unavailable",
            message: "request end settlement failed",
            retryable: false,
            fatal: false,
            sessionId: envelope.sessionId,
            writeId: envelope.writeId,
        },
    }));
    const result = await Effect.runPromise(Effect.gen(function* () {
        const threadLoop = yield* ThreadLoop.Service;
        return yield* threadLoop.run(session, testRunCustody());
    }).pipe(Effect.provide(runtimeThreadLoopLayer(loader, {
        writer,
        events: [
            { type: "reasoning-start", id: "reasoning-1", providerMetadata: { anthropic: { signature: "sig_uncommitted" } } },
            { type: "reasoning-delta", id: "reasoning-1", text_delta: "must not stabilize" },
            { type: "reasoning-end", id: "reasoning-1" },
            { type: "finish", finishReason: "stop" },
        ],
    }))));
    expect(result).toMatchObject({ type: "failed" });
    expect(session.state.contextManager.messages().flatMap((message) => message.parts).some((part) => part.type === "reasoning")).toBe(false);
});
test("retries a transient request-end failure with the identical ordered reasoning batch", async () => {
    const session = new ThreadRuntime("sesn_1");
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const attempts: SessionEventWriterRequestEndEnvelope[] = [];
    const writer = writerFrom((envelope) => ({ ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt }), async (envelope) => {
        attempts.push(structuredClone(envelope));
        expect(session.state.contextManager.messages().flatMap((message) => message.parts).some((part) => part.type === "reasoning")).toBe(false);
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
                    writeId: envelope.writeId,
                },
            };
        }
        return { ok: true, writeId: envelope.writeId, eventId: envelope.writeId, processedAt: createdAt };
    });
    const result = await Effect.runPromise(Effect.gen(function* () {
        const threadLoop = yield* ThreadLoop.Service;
        return yield* threadLoop.run(session, testRunCustody());
    }).pipe(Effect.provide(runtimeThreadLoopLayer(loader, {
        writer,
        events: [
            { type: "reasoning-start", id: "retry-reasoning-1" },
            { type: "reasoning-delta", id: "retry-reasoning-1", text_delta: "first" },
            { type: "reasoning-end", id: "retry-reasoning-1" },
            { type: "reasoning-start", id: "retry-reasoning-2" },
            { type: "reasoning-delta", id: "retry-reasoning-2", text_delta: "second" },
            { type: "reasoning-end", id: "retry-reasoning-2" },
            { type: "finish", finishReason: "stop" },
        ],
    }))));
    expect(result).toMatchObject({ type: "completed" });
    expect(attempts).toHaveLength(2);
    expect(attempts[1]).toEqual(attempts[0]);
    expect(attempts[0]?.stableReasoningParts?.map((part) => part.text)).toEqual(["first", "second"]);
    expect(session.state.contextManager.messages().flatMap((message) => message.parts).filter((part) => part.type === "reasoning")).toHaveLength(2);
});
test("discards completed reasoning when the provider attempt ends with a non-retryable error", async () => {
    const session = new ThreadRuntime("sesn_1");
    const transientAttachment = {
        transient: {
            attachmentRef: "att_failed_reasoning",
            sourceToolUseEventId: "sevt_failed_reasoning",
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
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
    const writer = writerFrom((envelope) => ({ ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt }), async (envelope) => {
        requestEnds.push(envelope);
        return { ok: true, writeId: envelope.writeId, eventId: envelope.writeId, processedAt: createdAt };
    });
    const result = await Effect.runPromise(Effect.gen(function* () {
        const threadLoop = yield* ThreadLoop.Service;
        return yield* threadLoop.run(session, testRunCustody());
    }).pipe(Effect.provide(runtimeThreadLoopLayer(loader, {
        writer,
        events: [
            { type: "reasoning-start", id: "failed-reasoning" },
            { type: "reasoning-delta", id: "failed-reasoning", text_delta: "discard me" },
            { type: "reasoning-end", id: "failed-reasoning" },
            {
                type: "provider-error",
                error: runtimeFailureFromProviderError(normalizeProviderError({
                    code: "provider_invalid_request",
                    message: "terminal provider failure",
                    retryable: false,
                    fatal: false,
                })),
            },
        ],
    }))));
    expect(result).toMatchObject({ type: "failed" });
    expect(requestEnds).toHaveLength(1);
    expect(requestEnds[0]).toMatchObject({ isError: true, errorKind: "provider_error" });
    expect(requestEnds[0]?.stableReasoningParts).toBeUndefined();
    expect(requestEnds[0]?.consumedAttachmentRefs ?? []).toEqual([]);
    expect(requestEnds[0]?.consumedFileAttachments ?? []).toEqual([]);
    expect(session.state.contextManager.messages().flatMap((message) => message.parts).some((part) => part.type === "reasoning")).toBe(false);
    expect(session.state.pendingAttachments()).toEqual(attachments);
});
test("task notification commits after the running receipt and reaches the provider once", async () => {
    const session = new ThreadRuntime("sesn_task_notification_turn");
    const order: string[] = [];
    const requests: LLMRequest[] = [];
    const notification = runtimeNotificationMessage("msg_task_notification_turn", "task result for next turn");
    session.state.recordBackgroundTool({
        taskId: "task_notification_turn",
        sourceToolUseEventId: "sevt_task_notification_tool",
    });
    expect(session.state.enqueueAcceptedInput({
        requestId: "req_task_notification_turn",
        ...session.identity,
        runtimeInputId: "rin_task_notification_turn",
        eventIds: [],
        sequenceFrom: 0,
        sequenceTo: 0,
        kind: "task_notification",
        taskId: "task_notification_turn",
        sourceToolUseEventId: "sevt_task_notification_tool",
        status: "completed",
        payloadJson: "{\"status\":\"completed\",\"text\":\"task result for next turn\"}",
        commit: async () => {
            order.push("task-commit");
            return { ok: true, committedMessage: notification };
        },
    })).toBe("applied");
    const writer = writerFrom((envelope) => {
        if (envelope.event.type === "session.status_running") {
            order.push("running-receipt");
        }
        return {
            ok: true,
            writeId: envelope.writeId,
            eventId: `bridge-${envelope.writeId}`,
            processedAt: createdAt,
        };
    });
    const loader = new RecordingContextLoader([], { type: "empty" });
    const result = await Effect.runPromise(Effect.gen(function* () {
        const threadLoop = yield* ThreadLoop.Service;
        return yield* threadLoop.run(session, testRunCustody());
    }).pipe(Effect.provide(runtimeThreadLoopLayer(loader, {
        writer,
        llmService: llmService([
            { type: "text-start", id: "answer-text" },
            { type: "text-delta", id: "answer-text", text_delta: "acknowledged" },
            { type: "text-end", id: "answer-text" },
            { type: "finish", finishReason: "stop" },
        ], (request) => {
            order.push("provider");
            requests.push(request);
        }),
    }))));
    expect(result).toMatchObject({ type: "completed" });
    expect(order.slice(0, 3)).toEqual(["running-receipt", "task-commit", "provider"]);
    expect(requests).toHaveLength(1);
    expect(JSON.stringify(requests[0]?.messages).match(/task result for next turn/g)).toHaveLength(1);
    expect(session.state.contextManager.messages().filter((message) => message.id === notification.id)).toHaveLength(1);
    expect(session.state.peekAcceptedInput()).toBeUndefined();
    expect(session.state.backgroundTool("task_notification_turn")).toMatchObject({
        status: "terminal",
        terminalNotification: expect.objectContaining({ runtimeInputId: "rin_task_notification_turn" }),
    });
    expect(session.state.threadTurnReduction().appliedEventIds).toContain(notification.owningEventId);
    expect(session.state.threadTurnReduction().appliedEventIds).not.toContain(notification.id);
});

test("prefix-only child Request Start stores durable message boundary zero", async () => {
    const session = new ThreadRuntime("sesn_1");
    const prefixMessage = userMessage("msg_parent_prefix", 41, "prefix-only child task");
    session.state.contextManager.installThreadContextPrefix({
        childThreadId: session.identity.sessionThreadId,
        parentThreadId: "thrd_parent",
        parentBoundaryEventId: "sevt_parent_boundary",
        entries: [prefixMessage],
        createdAt,
    });
    session.state.markPersistentContextLoaded();
    session.state.installThreadTurn({
        pendingInputMessageIds: [prefixMessage.id],
    }, { routes: [] });
    const requestStarts: number[] = [];
    const writer = writerFrom((envelope) => {
        if (envelope.event.type === "span.model_request_start") {
            requestStarts.push(envelope.contextThroughMessageSequence ?? Number.NaN);
        }
        return {
            ok: true,
            writeId: envelope.writeId,
            eventId: `bridge-${envelope.writeId}`,
            processedAt: createdAt,
        };
    });
    const result = await Effect.runPromise(Effect.gen(function* () {
        return yield* (yield* ThreadLoop.Service).run(session, testRunCustody());
    }).pipe(Effect.provide(runtimeThreadLoopLayer(new QueuedContextLoader([], []), {
        installLoaderState: false,
        writer,
    }))));

    expect(result).toMatchObject({ type: "completed" });
    expect(requestStarts).toEqual([0]);
});
test("task notification replays an unknown commit outcome before provider work", async () => {
    const session = new ThreadRuntime("sesn_task_notification_retryable_commit");
    const committedMessage = runtimeNotificationMessage("msg_task_notification_retryable_commit", "task result recovered from the replayed receipt");
    let commitCalls = 0;
    let providerCalls = 0;
    expect(session.state.enqueueAcceptedInput({
        requestId: "req_task_notification_retryable_commit",
        ...session.identity,
        runtimeInputId: "rin_task_notification_retryable_commit",
        eventIds: [],
        sequenceFrom: 0,
        sequenceTo: 0,
        kind: "task_notification",
        taskId: "task_notification_retryable_commit",
        sourceToolUseEventId: "sevt_task_notification_retryable_commit",
        status: "completed",
        payloadJson: "{\"status\":\"completed\"}",
        commit: async () => {
            commitCalls += 1;
            return commitCalls === 1
                ? {
                    ok: false as const,
                    retryable: true,
                    errorCode: "bridge_commit_unavailable",
                    message: "task notification durable commit failed",
                }
                : { ok: true as const, committedMessage };
        },
    })).toBe("applied");
    const custody = testRunCustody();
    const layer = runtimeThreadLoopLayer(new RecordingContextLoader([], { type: "empty" }), {
        onStream: () => {
            providerCalls += 1;
        },
    });
    const first = await Effect.runPromise(Effect.gen(function* () {
        const threadLoop = yield* ThreadLoop.Service;
        return yield* threadLoop.run(session, custody);
    }).pipe(Effect.provide(layer)));
    expect(first).toMatchObject({ type: "completed" });
    expect(commitCalls).toBe(1);
    expect(providerCalls).toBe(0);
    expect(session.state.peekAcceptedInput()).toMatchObject({
        kind: "task_notification",
        runtimeInputId: "rin_task_notification_retryable_commit",
    });
    expect(session.state.contextManager.messages()).toEqual([]);
    const second = await Effect.runPromise(Effect.gen(function* () {
        const threadLoop = yield* ThreadLoop.Service;
        return yield* threadLoop.run(session, custody);
    }).pipe(Effect.provide(layer)));
    expect(second).toMatchObject({ type: "completed" });
    expect(commitCalls).toBe(2);
    expect(providerCalls).toBe(1);
    expect(session.state.peekAcceptedInput()).toBeUndefined();
    expect(session.state.contextManager.messages()).toContainEqual(committedMessage);
});
test("task notification arriving during provider reschedule waits for the next durable turn", async () => {
    const session = new ThreadRuntime("sesn_task_notification_reschedule");
    const taskMessage = runtimeNotificationMessage("msg_task_notification_reschedule", "task result after the retried request");
    let commitCalls = 0;
    const requests: LLMRequest[] = [];
    const streams: readonly (readonly LLMEvent[])[] = [
        [{
                type: "provider-error",
                error: runtimeFailureFromProviderError(normalizeProviderError({
                    code: "provider_unavailable",
                    message: "retry the current request",
                    retryable: true,
                    fatal: false,
                })),
            }],
        [
            { type: "text-start", id: "current-answer" },
            { type: "text-delta", id: "current-answer", text_delta: "current turn recovered" },
            { type: "text-end", id: "current-answer" },
            { type: "finish", finishReason: "stop" },
        ],
        [
            { type: "text-start", id: "task-answer" },
            { type: "text-delta", id: "task-answer", text_delta: "task acknowledged" },
            { type: "text-end", id: "task-answer" },
            { type: "finish", finishReason: "stop" },
        ],
    ];
    let streamIndex = 0;
    const llm: LLMServiceInterface = {
        stream(request) {
            requests.push(request);
            if (streamIndex === 0) {
                expect(session.state.enqueueAcceptedInput({
                    requestId: "req_task_notification_reschedule",
                    ...session.identity,
                    runtimeInputId: "rin_task_notification_reschedule",
                    eventIds: [],
                    sequenceFrom: 0,
                    sequenceTo: 0,
                    kind: "task_notification",
                    taskId: "task_notification_reschedule",
                    sourceToolUseEventId: "sevt_task_notification_reschedule",
                    status: "completed",
                    payloadJson: "{\"status\":\"completed\"}",
                    commit: async () => {
                        commitCalls += 1;
                        return { ok: true, committedMessage: taskMessage };
                    },
                })).toBe("applied");
            }
            return Stream.fromIterable(streams[streamIndex++] ?? []);
        },
    };
    const layer = runtimeThreadLoopLayer(new RecordingContextLoader([], {
        type: "messages",
        messages: [userMessage("user-task-reschedule", 0, "finish the current request")],
    }), {
        llmService: llm,
        runtimePolicy: () => ({ providerRescheduleBudget: 3, compactionRescheduleBudget: 2 }),
    });
    const custody = testRunCustody();
    const currentTurn = await Effect.runPromise(Effect.gen(function* () {
        const threadLoop = yield* ThreadLoop.Service;
        return yield* threadLoop.run(session, custody);
    }).pipe(Effect.provide(layer)));
    expect(currentTurn).toMatchObject({ type: "completed" });
    expect(commitCalls).toBe(0);
    expect(requests).toHaveLength(2);
    expect(JSON.stringify(requests[1]?.messages)).not.toContain("task result after the retried request");
    expect(session.state.peekAcceptedInput()).toMatchObject({
        kind: "task_notification",
        runtimeInputId: "rin_task_notification_reschedule",
    });
    const taskTurn = await Effect.runPromise(Effect.gen(function* () {
        const threadLoop = yield* ThreadLoop.Service;
        return yield* threadLoop.run(session, custody);
    }).pipe(Effect.provide(layer)));
    expect(taskTurn).toMatchObject({ type: "completed" });
    expect(commitCalls).toBe(1);
    expect(requests).toHaveLength(3);
    expect(JSON.stringify(requests[2]?.messages).match(/task result after the retried request/g)).toHaveLength(1);
});
test("stale task notification custody discards the resident thread before provider work", async () => {
    const session = new ThreadRuntime("sesn_stale_task_notification");
    let providerCalls = 0;
    expect(session.state.enqueueAcceptedInput({
        requestId: "req_stale_task_notification",
        ...session.identity,
        runtimeInputId: "rin_stale_task_notification",
        eventIds: [],
        sequenceFrom: 0,
        sequenceTo: 0,
        kind: "task_notification",
        taskId: "task_stale_task_notification",
        sourceToolUseEventId: "sevt_stale_task_notification",
        status: "completed",
        payloadJson: "{\"status\":\"completed\"}",
        commit: async () => ({ ok: true, stale: true }),
    })).toBe("applied");
    const result = await Effect.runPromise(Effect.gen(function* () {
        const threadLoop = yield* ThreadLoop.Service;
        return yield* threadLoop.run(session, testRunCustody());
    }).pipe(Effect.provide(runtimeThreadLoopLayer(new RecordingContextLoader([], { type: "empty" }), {
        onStream: () => {
            providerCalls += 1;
        },
    }))));
    expect(result).toEqual({ type: "interrupted", discardHotState: true });
    expect(providerCalls).toBe(0);
    expect(session.state.peekAcceptedInput()).toBeUndefined();
    expect(session.state.contextManager.messages()).toEqual([]);
});
test("denied provider reschedule appends one exhausted error before idle", async () => {
    const order: string[] = [];
    const session = new ThreadRuntime("sesn_1");
    const store = new ThreadLoopRuntimeStore(order);
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const appended: SessionEvent[] = [];
    const baseWriter = writerFrom((envelope) => {
        appended.push(envelope.event);
        return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
    });
    const writer: SessionEventWriter = {
        ...baseWriter,
        writeRequestEnd: async (envelope) => {
            const result = await baseWriter.writeRequestEnd(envelope);
            return result.ok
                ? {
                    ...result,
                    rescheduleDisposition: {
                        status: "denied",
                        reason: "budget_exhausted",
                        attempt: envelope.reschedule?.attempt ?? 0,
                    },
                }
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
    const result = await Effect.runPromise(Effect.gen(function* () {
        const threadLoop = yield* ThreadLoop.Service;
        return yield* threadLoop.run(session, testRunCustody());
    }).pipe(Effect.provide(runtimeThreadLoopLayer(loader, {
        store,
        writer,
        events: [{ type: "provider-error", error: providerError }],
        runtimePolicy: () => ({ providerRescheduleBudget: 3, compactionRescheduleBudget: 2 }),
    }))));
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
    expect(appended.at(4)).toEqual({ type: "session.status_idle", stop_reason: { type: "retries_exhausted" } });
    expect(JSON.stringify(appended)).not.toContain('"type":"retrying"');
    expect(session.state.contextManager.messages().some((message) => message.role === "assistant")).toBe(false);
});
});

describe("provider call skill guidance", () => {
    test("rejects an unspecified provider request kind", () => {
        expect(assembleProviderCallRequest(providerInput([], ProviderRequestKind.PROVIDER_REQUEST_KIND_UNSPECIFIED))).toMatchObject({ ok: false, error: { reason: "runtime_contract_validation" } });
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
        expect(agent.system[1]?.text.indexOf('"name":"Alpha"')).toBeLessThan(agent.system[1]?.text.indexOf('"name":"Zeta"') ?? -1);
        expect(agent.system[1]?.text).toContain('"skill_md_path":"/skills/alpha/SKILL.md"');
        expect(agent.system[1]?.text).not.toContain("skill_version_id");
        expect(agent.system[1]?.text).not.toContain("skill_id");
        expect(agent.system[1]?.text).not.toContain("skill body contents");
        for (const requestKind of [
            ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
            ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY,
            ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER_COMPACTION,
        ]) {
            const nonAgent = assembleProviderCallRequest(providerInput(skillsIndex, requestKind));
            expect(nonAgent.ok).toBe(true);
            if (nonAgent.ok) {
                expect(nonAgent.system.map((segment) => segment.kind)).not.toContain(SystemSegmentKind.SYSTEM_SEGMENT_KIND_SKILL);
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
                instructions: "Keep decisions and durable context here.\nPreserve this line verbatim.",
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
                expect(nonAgent.system.map((segment) => segment.kind)).not.toContain(SystemSegmentKind.SYSTEM_SEGMENT_KIND_AGENT);
                expect(nonAgent.system.map((segment) => segment.text)).not.toContain(agentSystem);
                expect(nonAgent.system.map((segment) => segment.kind)).not.toContain(SystemSegmentKind.SYSTEM_SEGMENT_KIND_MEMORY);
            }
        }
    });
    test("keeps the platform base system prompt byte-for-byte stable", () => {
        expect(PlatformBaseSystemPrompt).toBe("You are Tetral Agent, working in a sandboxed Linux environment.\n\nFiles in your sandbox persist for the life of this session,\nincluding across session sleep and wake, and are gone when the\nsession ends. To keep something across sessions, use the memory\ntool. To deliver a file to the user, save it under\n/mnt/session/outputs — files there are collected and delivered\nautomatically.");
    });
    test("injects bounded apply-patch instructions only for GPT-family agent requests", () => {
        expect(new TextEncoder().encode(ApplyPatchInstructionsText).byteLength).toBeLessThan(MaxTextBytes);
        expect(ApplyPatchInstructionsText).toContain("Absolute paths are accepted under the declared writable roots — the workspace, /mnt/session/uploads, and /mnt/session/outputs — and rejected outside them.");
        for (const [family, requestKind, shouldInject] of [
            ["gpt", ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST, true],
            ["claude", ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST, false],
            [undefined, ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST, false],
            ["gpt", ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER, false],
            ["gpt", ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY, false],
        ] as const) {
            const input = providerInput([], requestKind);
            const result = assembleProviderCallRequest({
                ...input,
                runtime: { ...input.runtime, ...(family === undefined ? {} : { toolsetFamily: family }) },
            });
            expect(result.ok).toBe(true);
            if (!result.ok) {
                continue;
            }
            const base = result.system.find((segment) => segment.kind === SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE)?.text ?? "";
            expect(base.includes(ApplyPatchInstructionsText)).toBe(shouldInject);
        }
    });
    test("rejects a GPT base prompt whose apply-patch injection exceeds the segment cap", () => {
        const input = providerInput([]);
        expect(assembleProviderCallRequest({
            ...input,
            runtime: {
                ...input.runtime,
                toolsetFamily: "gpt",
                systemInstructions: "x".repeat(MaxTextBytes),
            },
        })).toMatchObject({ ok: false, error: { reason: "bounded" } });
    });
    test("adds the approval policy as a dedicated stable segment only to reviewer requests", () => {
        const policy = "Review solely under this fixed approval policy.";
        const reviewer = assembleProviderCallRequest(providerInput([], ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER, policy));
        expect(reviewer.ok).toBe(true);
        if (!reviewer.ok) {
            return;
        }
        expect(reviewer.system.filter((segment) => segment.kind === SystemSegmentKind.SYSTEM_SEGMENT_KIND_APPROVAL_REVIEWER_POLICY)).toEqual([{
                kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_APPROVAL_REVIEWER_POLICY,
                text: policy,
                cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_STABLE,
            }]);
        expect(reviewer.system.find((segment) => segment.kind === SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE)?.text).not.toContain(policy);
        for (const requestKind of [
            ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
            ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY,
            ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER_COMPACTION,
        ]) {
            const nonReviewer = assembleProviderCallRequest(providerInput([], requestKind, policy));
            expect(nonReviewer.ok).toBe(true);
            if (nonReviewer.ok) {
                expect(nonReviewer.system.map((segment) => segment.kind)).not.toContain(SystemSegmentKind.SYSTEM_SEGMENT_KIND_APPROVAL_REVIEWER_POLICY);
                expect(nonReviewer.system.map((segment) => segment.text)).not.toContain(policy);
            }
        }
    });
    test("rejects reviewer requests without a non-empty dedicated approval policy", () => {
        const withPolicy = providerInput([], ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER);
        const { approvalReviewerPolicy: _removedPolicy, ...runtimeWithoutPolicy } = withPolicy.runtime;
        for (const input of [
            { ...withPolicy, runtime: runtimeWithoutPolicy },
            { ...withPolicy, runtime: { ...withPolicy.runtime, approvalReviewerPolicy: "   " } },
        ]) {
            const result = assembleProviderCallRequest(input);
            expect(result).toMatchObject({
                ok: false,
                error: { reason: "runtime_contract_validation" },
            });
        }
    });
    test("assembles reviewer compaction without system segments, tools, or output schema", () => {
        const input = providerInput([], ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER_COMPACTION);
        const result = assembleProviderCallRequest(input);
        expect(result.ok).toBe(true);
        if (!result.ok) {
            return;
        }
        expect(result.request).toMatchObject({
            requestKind: ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER_COMPACTION,
            system: [],
            tools: [],
        });
        expect(result.request.outputSchemaJson).toBeUndefined();
        expect(assembleProviderCallRequest({
            ...input,
            runtime: { ...input.runtime, outputSchemaJson: '{"type":"object"}' },
        })).toMatchObject({ ok: false, error: { reason: "runtime_contract_validation" } });
    });
    test("requires the Runtime-configured provider stream timeout", () => {
        const input = providerInput([]);
        const { timeoutMs: _timeoutMs, ...runtimeWithoutTimeout } = input.runtime;
        expect(assembleProviderCallRequest({ ...input, runtime: runtimeWithoutTimeout })).toMatchObject({
            ok: false,
            error: { reason: "bounded" },
        });
    });
    test("applies and notes per-entry and uniform description truncation", () => {
        const text = renderSkillGuidanceSegment([
            skillEntry("sk_alpha", "1.0.0", "Alpha", "alpha", "a".repeat(5000)),
            skillEntry("sk_beta", "1.0.0", "Beta", "beta", "b".repeat(5000)),
            skillEntry("sk_gamma", "1.0.0", "Gamma", "gamma", "界".repeat(5000)),
        ], 1024);
        expect(text).toContain("per-entry description cap applied");
        expect(text).toContain("uniform description shortening applied");
        const descriptionBytes = text
            .split("\n")
            .filter((line) => line.startsWith("{"))
            .map((line) => JSON.parse(line) as {
            readonly description: string;
        })
            .reduce((total, entry) => total + new TextEncoder().encode(entry.description).byteLength, 0);
        expect(descriptionBytes).toBeLessThanOrEqual(1024);
        expect(new TextEncoder().encode(text).byteLength).toBeLessThan(MaxTextBytes);
    });
    test("omits entries from the deterministic tail and notes the omission", () => {
        const text = renderSkillGuidanceSegment([
            skillEntry("sk_alpha", "1.0.0", "a".repeat(30000), "alpha", ""),
            skillEntry("sk_beta", "1.0.0", "b".repeat(30000), "beta", ""),
            skillEntry("sk_zeta", "1.0.0", "z".repeat(30000), "zeta", ""),
        ], 1024);
        expect(text).toContain('"skill_md_path":"/skills/alpha/SKILL.md"');
        expect(text).not.toContain('"skill_md_path":"/skills/zeta/SKILL.md"');
        expect(text).toContain("end-of-order skill omission applied");
        expect(new TextEncoder().encode(text).byteLength).toBeLessThan(MaxTextBytes);
    });
});
function providerInput(skillsIndex: readonly SkillGuidanceIndexEntry[], requestKind = ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST, approvalReviewerPolicy = requestKind === ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER
    ? "Fixed approval reviewer policy."
    : undefined): ProviderCallAssemblyInput {
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
        runtimeMessages: [{
                id: "message_1",
                role: RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_USER,
                status: "completed",
                origin: "user",
                parts: [{ id: "part_1", text: { text: "hello" } }],
            }],
        runtime: {
            systemInstructions: "You are Tetral Agent.",
            timeoutMs: 1800000,
            requestKind,
            skillsIndex,
            skillGuidanceDescriptionBudgetBytes: 32 * 1024,
            ...(approvalReviewerPolicy !== undefined ? { approvalReviewerPolicy } : {}),
            ...(requestKind === ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER
                ? { outputSchemaJson: '{"type":"object"}' }
                : {}),
        },
    };
}
function skillEntry(skillId: string, version: string, name: string, directory: string, description: string): SkillGuidanceIndexEntry {
    return {
        skillId,
        skillVersionId: `skv_${skillId}_${version}`,
        version,
        name,
        description,
        directory,
    };
}
