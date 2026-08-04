import { describe, expect, jest, test } from "bun:test";
import { readFile } from "node:fs/promises";
import { Cause, Context, Effect, Exit, Fiber, Layer, Scope, Stream } from "effect";
import { ProviderRequestKind, RuntimeMessageRole, SystemCacheHint, SystemSegmentKind, } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { DurableRuntimeMessage, PendingInputResult, RuntimeDependencies, RuntimeInternalToolRepairCommit, RuntimeMessage, RuntimeMessageDraft, RuntimeMessageInfo, RuntimeDeclarationOperationControls, RuntimePart, SessionEvent, SessionEventEnvelope, SessionEventWriter, SessionEventWriterAppendResult, SessionEventWriterFinishIdleEnvelope, SessionEventWriterRequestEndEnvelope, SessionEventWriterRuntimeTerminationEnvelope, } from "../../../src/contracts/runtime.js";
import { DurableRuntimeMessageSchema, RuntimeMessageSchema, RuntimeInternalToolRepairStore, SessionEventWriterRetryPolicy, normalizeContextLoaderError, normalizeRuntimeMessageStoreError, normalizeSessionEventWriterError, } from "../../../src/contracts/runtime.js";
import { LLMEventSchema } from "../../../src/llm/llm-event.js";
import type { LLMEvent, RuntimeUsage } from "../../../src/llm/llm-event.js";
import { runtimeFailureFromProviderError } from "../../../src/llm/llm-event.js";
import type { Interface as LLMServiceInterface, LLMRequest, LLMServiceError } from "../../../src/llm/llm-service.js";
import type { ProviderCallAssembler, ProviderCallRuntimeConfig } from "../../../src/thread-loop/provider-request.js";
import { DefaultProviderCallRuntimeConfig, assembleProviderCallRequest } from "../../../src/thread-loop/provider-request.js";
import { compactionBoundaryMessageSequence } from "../../../src/thread-loop/compaction.js";
import { createToolCatalog } from "../../../src/tools/tool-catalog.js";
import type { ToolCatalog } from "../../../src/tools/tool-catalog.js";
import * as ThreadLoop from "../../../src/thread-loop/thread-loop.js";
import * as SessionManager from "../../../src/session/session-manager.js";
import type { RuntimeContextLoadOperation, RuntimeEventWriteOperation, RuntimeHotStateMetrics, RuntimeMetricOutcome, RuntimeMetricsSink, RuntimeProviderStreamKind, } from "../../../src/runtime/metrics.js";
import type { AcceptedInputCommitResult, ContextLoader } from "../../../src/context/context-loader.js";
import { normalizeProviderError } from "../../../src/contracts/provider.js";
import { ThreadRuntime } from "../../../src/thread-loop/thread-runtime.js";
import type { RuntimeToolExecutionResult } from "../../../src/thread-loop/tool-execution.js";
import { AutoApprovalReviewerManager } from "../../../src/session/approval-reviewer-manager.js";
import type { RuntimeAcceptedInputState, RuntimeConfigPatchState, RuntimeControlInputDeclaration, } from "../../../src/thread-loop/thread-state.js";
import { SessionProcessor } from "../../../src/runtime/accumulator.js";
import { SessionToolCoordinator } from "../../../src/tools/tool-scheduler.js";
import { runtimeModelForThread, runtimeToolPolicyFromPatchPayloads } from "../../../../runtime-pod/src/command.js";
import { buildThreadLoopUserMessage as userMessage, buildThreadLoopRuntimeNotificationMessage as runtimeNotificationMessage, buildRuntimeControlCommitResult, } from "../runtime-message-builders.js";
import { acceptedInputReceipt } from "../runtime-declaration-fixtures.js";

interface PackageJson {
    readonly dependencies?: Readonly<Record<string, string>>;
}

const createdAt = "2026-06-14T00:00:00.000Z";

const emptyColdCoverage = {
    pendingToolIds: [],
    pendingSandboxExecutionIds: [],
    pendingAttachmentIdentities: [],
    undeliveredMailDeliveryIds: [],
} as const;

const approvalReviewerOutputSchemaJson = JSON.stringify({
    type: "object",
    additionalProperties: false,
    required: ["risk_level", "user_authorization", "outcome", "rationale"],
    properties: {
        risk_level: { enum: ["low", "medium", "high", "critical"] },
        user_authorization: { enum: ["unknown", "low", "medium", "high"] },
        outcome: { enum: ["allow", "deny"] },
        rationale: { type: "string" },
    },
});

const approvalReviewerPolicy = "Fixed approval reviewer policy for tests.";

const ProviderDiagnosticCanaries = [
    "opaqueDummySecret123",
    "raw-provider-json-with-user-prompt",
    "safeLookingProviderPayload",
] as const;

function expectNoProviderDiagnosticCanaries(value: unknown): void {
    const serialized = JSON.stringify(value);
    for (const canary of ProviderDiagnosticCanaries) {
        expect(serialized).not.toContain(canary);
    }
    expect(serialized).not.toContain("responseHeaders");
    expect(serialized).not.toContain("responseBody");
    expect(serialized).not.toContain("rawBody");
    expect(serialized).not.toContain("stack");
}

function acceptedInput(runtimeInputId = "rin_follow_up", sessionId = "sesn_1"): RuntimeAcceptedInputState {
    return {
        requestId: `req_${runtimeInputId}`,
        workspaceId: "wksp_test",
        sessionId,
        sessionThreadId: "thrd_1",
        bindingId: "bind_1",
        bindingGeneration: 1,
        targetPodUid: "pod_1",
        runtimeInputId,
        eventIds: [`sevt_${runtimeInputId}`],
        sequenceFrom: 1,
        sequenceTo: 1,
        kind: "messages",
        payloadJson: JSON.stringify({
            messages: [userMessage(`msg_${runtimeInputId}`, 1, "test input")],
        }),
    };
}

function installRecoveredToolTurn(session: ThreadRuntime, modelRequestId: string, members: ReadonlyArray<{
    readonly modelToolCallId: string;
    readonly toolUseEventId: string;
    readonly toolName: string;
    readonly disposition?: "requires_user_action" | "resume_approval_settlement" | "resume_sandbox_execution";
}>): void {
    session.state.installThreadTurn({
        pendingInputMessageIds: [],
        request: {
            modelRequestId,
            requestStartEventId: `${modelRequestId}_start`,
            requestKind: "agent_provider_request",
            contextThroughMessageSequence: 0,
            toolMembers: members.map(({ disposition: _disposition, ...member }) => ({
                memberKind: "public_tool_use" as const,
                ...member,
            })),
            requestEnd: {
                eventId: `${modelRequestId}_end`,
                isError: false,
                rescheduled: false,
            },
        },
    }, {
        routes: members.map((member) => ({
            toolUseEventId: member.toolUseEventId,
            disposition: member.disposition ?? "requires_user_action",
        })),
    });
}

function testControlCommit(scope: RuntimeAcceptedInputState, inputKind: "interrupt_control" | "tool_confirmation" = "interrupt_control") {
    return async (declaration: RuntimeControlInputDeclaration) => buildRuntimeControlCommitResult(scope, inputKind, declaration);
}

function beginTestUserInterrupt(session: ThreadRuntime, runtimeInputId: string, onCommit: () => void = () => { }): void {
    const scope = {
        ...acceptedInput(runtimeInputId),
        workspaceId: session.identity.workspaceId,
        sessionThreadId: session.identity.sessionThreadId,
        bindingId: session.identity.bindingId,
        bindingGeneration: session.identity.bindingGeneration,
        targetPodUid: session.identity.targetPodUid,
        eventIds: [`sevt_${runtimeInputId}`],
        sequenceFrom: 9,
        sequenceTo: 9,
    };
    session.state.beginUserInterrupt(scope, async (declaration) => {
        onCommit();
        return buildRuntimeControlCommitResult(scope, "interrupt_control", declaration);
    });
}

function approvalReviewAcceptedInput(runtimeInputId = "rin_approval_review"): Extract<RuntimeAcceptedInputState, {
    readonly kind: "approval_review";
}> {
    return {
        requestId: `req_${runtimeInputId}`,
        workspaceId: "wksp_reviewer",
        sessionId: "sesn_1",
        sessionThreadId: "thrd_reviewer",
        bindingId: "bind_reviewer",
        bindingGeneration: 1,
        targetPodUid: "pod_reviewer",
        runtimeInputId,
        eventIds: [`sevt_${runtimeInputId}`],
        sequenceFrom: 1,
        sequenceTo: 1,
        kind: "approval_review",
        reviewId: `arvw_${runtimeInputId}`,
        parentThreadId: "thrd_main",
        targetModelToolCallId: `tool_call_${runtimeInputId}`,
        targetToolName: "Write",
        promptItems: [userMessage(`review-${runtimeInputId}`, 0, "review pending tool approval")],
        outputSchemaJson: approvalReviewerOutputSchemaJson,
        thread: {
            parentThreadId: "thrd_main",
            role: "approval_reviewer",
            visibility: "internal",
            agentType: "approval_reviewer",
            status: "idle",
        },
    };
}

interface TestContextLoader extends ContextLoader {
    readonly buildContext: (sessionId: string) => Promise<readonly RuntimeMessage[]>;
    readonly loadPendingInput: (sessionId: string) => Promise<PendingInputResult>;
}

class RecordingContextLoader implements TestContextLoader {
    readonly buildCalls: string[] = [];
    readonly pendingCalls: string[] = [];
    private nextMessageSequence = 1;
    constructor(private readonly history: readonly RuntimeMessage[], private readonly pending: PendingInputResult) { }
    async buildContext(sessionId: string): Promise<readonly RuntimeMessage[]> {
        this.buildCalls.push(sessionId);
        return this.history;
    }
    async loadPendingInput(sessionId: string): Promise<PendingInputResult> {
        this.pendingCalls.push(sessionId);
        return this.pending;
    }
    async commitAcceptedInput(input: RuntimeAcceptedInputState): Promise<AcceptedInputCommitResult> {
        const result = acceptedInputReceipt(input, "committed", this.nextMessageSequence);
        this.nextMessageSequence += result.receipt.messages.length;
        return result;
    }
}

class QueuedContextLoader implements TestContextLoader {
    readonly buildCalls: string[] = [];
    readonly pendingCalls: string[] = [];
    readonly commitCalls: RuntimeAcceptedInputState[] = [];
    private nextMessageSequence = 1;
    constructor(private readonly history: readonly RuntimeMessage[], private readonly pendingResults: PendingInputResult[], private readonly acceptedResults: Array<unknown | ((input: RuntimeAcceptedInputState) => unknown)> = []) { }
    async buildContext(sessionId: string): Promise<readonly RuntimeMessage[]> {
        this.buildCalls.push(sessionId);
        return this.history;
    }
    async loadPendingInput(sessionId: string): Promise<PendingInputResult> {
        this.pendingCalls.push(sessionId);
        return this.pendingResults.shift() ?? { type: "empty" };
    }
    async commitAcceptedInput(input: RuntimeAcceptedInputState): Promise<AcceptedInputCommitResult> {
        this.commitCalls.push(input);
        const result = this.acceptedResults.shift();
        if (typeof result === "function") {
            return result(input) as AcceptedInputCommitResult;
        }
        if (result !== undefined) {
            return result as AcceptedInputCommitResult;
        }
        const committed = acceptedInputReceipt(input, "committed", this.nextMessageSequence);
        this.nextMessageSequence += committed.receipt.messages.length;
        return committed;
    }
}

let testAcceptedInputSequence = 0;

function installPendingInputForTest(session: ThreadRuntime, pending: PendingInputResult): void {
    if (session.state.peekAcceptedInput() !== undefined ||
        pending.type !== "messages" ||
        pending.messages.length === 0) {
        return;
    }
    testAcceptedInputSequence += 1;
    const runtimeInputId = `rin_test_harness_${testAcceptedInputSequence}`;
    const eventIds = pending.messages.map((_, index) => `sevt_${runtimeInputId}_${index}`);
    session.state.enqueueAcceptedInput({
        requestId: `req_${runtimeInputId}`,
        ...session.identity,
        runtimeInputId,
        eventIds,
        sequenceFrom: testAcceptedInputSequence,
        sequenceTo: testAcceptedInputSequence + eventIds.length - 1,
        kind: "messages",
        payloadJson: JSON.stringify({ messages: pending.messages }),
    });
}

async function installLoaderStateForTest(loader: TestContextLoader, session: ThreadRuntime): Promise<void> {
    if (!session.state.persistentContextLoaded()) {
        session.state.contextManager.replaceMessages(await loader.buildContext(session.sessionId));
        session.state.markPersistentContextLoaded();
    }
    if (session.state.peekAcceptedInput() === undefined) {
        installPendingInputForTest(session, await loader.loadPendingInput(session.sessionId));
    }
}

class ThreadLoopRuntimeStore extends RuntimeInternalToolRepairStore {
    readonly messages = new Map<string, RuntimeMessage>();
    readonly repairs: RuntimeInternalToolRepairCommit[] = [];
    constructor(private readonly order: string[], private readonly failPartWrite: boolean | ((part: RuntimePart) => boolean) = false, private readonly failMessageWrite = false, private readonly beforeWrite?: (operation: "writeMessage" | "writePart", payload: RuntimeMessageInfo | RuntimePart) => void, private readonly beforeInternalToolRepair?: (repair: RuntimeInternalToolRepairCommit) => void | Promise<void>, private readonly durableSequence?: TestDurableSequence) {
        super();
    }
    protected async writeMessageRecord(message: RuntimeMessageInfo, _controls: RuntimeDeclarationOperationControls): Promise<unknown> {
        this.beforeWrite?.("writeMessage", message);
        this.order.push(`store:message:${message.status}`);
        if (this.failMessageWrite) {
            return {
                ok: false,
                error: normalizeRuntimeMessageStoreError({
                    code: "unavailable",
                    operation: "commitInternalToolRepair",
                    sessionId: message.sessionId,
                    messageId: message.id,
                }),
            };
        }
        const existing = this.messages.get(message.id);
        this.messages.set(message.id, RuntimeMessageSchema.parse({ ...message, parts: existing?.parts ?? [] }));
        return { ok: true, messageId: message.id, operation: "commitInternalToolRepair" };
    }
    protected async writePartRecord(part: RuntimePart, _controls: RuntimeDeclarationOperationControls): Promise<unknown> {
        this.beforeWrite?.("writePart", part);
        this.order.push(`store:part:${part.type}`);
        if (this.failPartWrite === true || (typeof this.failPartWrite === "function" && this.failPartWrite(part))) {
            return {
                ok: false,
                error: normalizeRuntimeMessageStoreError({
                    code: "unavailable",
                    operation: "commitInternalToolRepair",
                    sessionId: part.sessionId,
                    messageId: part.messageId,
                    partId: part.id,
                }),
            };
        }
        const existing = this.messages.get(part.messageId);
        if (existing === undefined) {
            throw new Error("missing assistant shell");
        }
        this.messages.set(existing.id, RuntimeMessageSchema.parse({
            ...existing,
            parts: [...existing.parts.filter((current) => current.id !== part.id), part].sort((left, right) => left.sequence - right.sequence),
        }));
        return { ok: true, messageId: part.messageId, partId: part.id, operation: "commitInternalToolRepair" };
    }
    protected async commitInternalToolRepairRecord(repair: RuntimeInternalToolRepairCommit): Promise<unknown> {
        await this.beforeInternalToolRepair?.(repair);
        const part = repair.draft.parts[0];
        if (part === undefined) {
            throw new Error("missing internal repair part");
        }
        this.repairs.push(repair);
        this.order.push("store:internal-tool-repair");
        const eventId = `event-internal-repair-${this.repairs.length}`;
        const messageId = `message-internal-repair-${this.repairs.length}`;
        const partId = `part-internal-repair-${this.repairs.length}`;
        const eventSequence = this.durableSequence === undefined
            ? 20 + this.repairs.length
            : ++this.durableSequence.eventSequence;
        const messageSequence = this.durableSequence === undefined
            ? 2
            : ++this.durableSequence.messageSequence;
        return {
            ok: true,
            eventId,
            declaration: {
                applicationDisposition: "current_custody",
                observedBindingId: repair.bindingId,
                observedBindingGeneration: repair.bindingGeneration,
                receipt: {
                    sessionThreadId: repair.sessionThreadId,
                    operationKind: "commit_internal_tool_repair",
                    sourceKind: "internal_tool_repair",
                    sourceId: repair.repairKey,
                    declarationDigest: "test-repair-digest",
                    pendingAttachmentDelta: [],
                    events: [{
                            sessionThreadId: repair.sessionThreadId,
                            sourceEventId: repair.repairKey,
                            eventId,
                            eventSequence,
                            disposition: "created",
                        }],
                    messages: [{
                            runtimeLocalId: repair.draft.runtimeLocalId,
                            sessionThreadId: repair.sessionThreadId,
                            owningEventId: eventId,
                            messageId,
                            messageSequence,
                            createdAt,
                            updatedAt: createdAt,
                            disposition: "created",
                            parts: [{
                                    runtimeLocalPartId: part.runtimeLocalPartId,
                                    partId,
                                    messageId,
                                    partSequence: 0,
                                    createdAt,
                                    updatedAt: createdAt,
                                    disposition: "created",
                                }],
                        }],
                },
            },
        };
    }
}

function threadLoopRuntime() {
    let counter = 0;
    return {
        now: () => createdAt,
        monotonicMs: () => 0,
        createId: (prefix: string) => `${prefix}-${++counter}`,
        sleep: async () => true,
    } satisfies RuntimeDependencies;
}

function testRunCustody(initialDurableTurnId?: string): ThreadLoop.ThreadLoopRunCustody {
    let durableTurnId = initialDurableTurnId;
    return {
        durableTurnId: () => durableTurnId,
        recordDurableTurnId: (value) => {
            durableTurnId = value;
        },
        closeDurableTurn: (value) => {
            if (durableTurnId === value) {
                durableTurnId = undefined;
            }
        },
    };
}

async function sleepUntilAborted(_milliseconds: number, signal: AbortSignal): Promise<boolean> {
    return await new Promise<boolean>((resolve) => {
        if (signal.aborted) {
            resolve(false);
            return;
        }
        signal.addEventListener("abort", () => resolve(false), { once: true });
    });
}

function llmService(events: readonly LLMEvent[], onStream?: (request: LLMRequest) => void): LLMServiceInterface {
    return {
        stream(request) {
            onStream?.(request);
            return Stream.fromIterable(events);
        },
    };
}

function firstRequestThenFinalResponse(events: readonly LLMEvent[], onStream?: (request: LLMRequest) => void): LLMServiceInterface {
    let requestCount = 0;
    return {
        stream(request) {
            onStream?.(request);
            requestCount += 1;
            return Stream.fromIterable(requestCount === 1
                ? events
                : [
                    { type: "text-start" as const, id: "continuation-final" },
                    { type: "text-delta" as const, id: "continuation-final", text_delta: "done" },
                    { type: "text-end" as const, id: "continuation-final" },
                    { type: "finish" as const, finishReason: "stop" as const },
                ]);
        },
    };
}

function queuedLLMService(eventBatches: readonly (readonly LLMEvent[])[], requests: LLMRequest[] = []): LLMServiceInterface {
    let index = 0;
    return {
        stream(request) {
            requests.push(request);
            const events = eventBatches[index] ?? [];
            index += 1;
            return Stream.fromIterable(events);
        },
    };
}

interface TestDurableSequence {
    eventSequence: number;
    messageSequence: number;
}

class TestRuntimeDeclarationReceipts {
    constructor(private readonly durableSequence: TestDurableSequence = {
        eventSequence: 100000,
        messageSequence: 100000,
    }) { }
    private readonly activeMessages = new Map<string, {
        readonly messageId: string;
        readonly messageSequence: number;
        readonly owningEventId: string;
        readonly parts: Map<string, {
            readonly partId: string;
            readonly createdAt: string;
        }>;
    }>();
    seedMessage(sessionThreadId: string, message: DurableRuntimeMessage): void {
        this.durableSequence.eventSequence = Math.max(this.durableSequence.eventSequence, message.eventSequence);
        this.durableSequence.messageSequence = Math.max(this.durableSequence.messageSequence, message.sequence);
        this.activeMessages.set(sessionThreadId, {
            messageId: message.id,
            messageSequence: message.sequence,
            owningEventId: message.owningEventId,
            parts: new Map(message.parts.map((part) => [
                part.type === "tool"
                    ? `tool:${part.toolCallId}`
                    : part.type === "reasoning"
                        ? `reasoning:${part.providerPartId ?? part.sequence}`
                        : `${part.type}:${part.sequence}`,
                { partId: part.id, createdAt: part.createdAt },
            ])),
        });
    }
    apply(envelope: SessionEventEnvelope, result: SessionEventWriterAppendResult): SessionEventWriterAppendResult {
        if (envelope.event.type === "span.model_request_start") {
            this.activeMessages.delete(envelope.sessionThreadId);
        }
        if (!result.ok || result.declaration !== undefined) {
            return result;
        }
        this.durableSequence.eventSequence += 1;
        const messages = envelope.drafts.length === 0
            ? []
            : (() => {
                const current = this.activeMessages.get(envelope.sessionThreadId);
                const message = current ?? {
                    messageId: `message-${envelope.sessionThreadId}-${this.durableSequence.messageSequence + 1}`,
                    messageSequence: this.durableSequence.messageSequence + 1,
                    owningEventId: result.eventId,
                    parts: new Map<string, {
                        readonly partId: string;
                        readonly createdAt: string;
                    }>(),
                };
                if (current === undefined) {
                    this.durableSequence.messageSequence += 1;
                    this.activeMessages.set(envelope.sessionThreadId, message);
                }
                const messageDisposition = current === undefined ? "created" as const : "updated" as const;
                return envelope.drafts.map((draft) => ({
                    runtimeLocalId: draft.runtimeLocalId,
                    sessionThreadId: envelope.sessionThreadId,
                    owningEventId: message.owningEventId,
                    messageId: message.messageId,
                    messageSequence: message.messageSequence,
                    createdAt,
                    updatedAt: createdAt,
                    disposition: messageDisposition,
                    parts: draft.parts.map((part, partIndex) => {
                        const association = testPartAssociation(part);
                        const existing = message.parts.get(association);
                        const durablePart = existing ?? {
                            partId: `part-${message.messageId}-${message.parts.size + 1}`,
                            createdAt,
                        };
                        message.parts.set(association, durablePart);
                        return {
                            runtimeLocalPartId: part.runtimeLocalPartId,
                            partId: durablePart.partId,
                            messageId: message.messageId,
                            partSequence: partIndex,
                            createdAt: durablePart.createdAt,
                            updatedAt: createdAt,
                            disposition: existing === undefined ? "created" as const : "updated" as const,
                        };
                    }),
                }));
            })();
        const withReceipt: SessionEventWriterAppendResult = {
            ...result,
            declaration: {
                applicationDisposition: "current_custody",
                observedBindingId: envelope.bindingId,
                observedBindingGeneration: envelope.bindingGeneration,
                receipt: {
                    sessionThreadId: envelope.sessionThreadId,
                    operationKind: "write_event",
                    sourceKind: envelope.event.type,
                    sourceId: envelope.writeId,
                    declarationDigest: `digest-${envelope.writeId}`,
                    pendingAttachmentDelta: [],
                    pendingToolDelta: [],
                    prefixConsumptions: [],
                    childLifecycle: [],
                    events: [{
                            sessionThreadId: envelope.sessionThreadId,
                            sourceEventId: envelope.writeId,
                            eventId: result.eventId,
                            eventSequence: this.durableSequence.eventSequence,
                            disposition: "created",
                        }],
                    messages,
                },
            },
        };
        return withReceipt;
    }
    applyRequestEnd(envelope: SessionEventWriterRequestEndEnvelope, result: SessionEventWriterAppendResult): SessionEventWriterAppendResult {
        if (!result.ok || result.declaration !== undefined) {
            return result;
        }
        this.durableSequence.eventSequence += 1;
        const requestEndEventSequence = this.durableSequence.eventSequence;
        const requestEndEventId = result.eventId;
        const events = [{
                sessionThreadId: envelope.sessionThreadId,
                sourceEventId: envelope.modelRequestId,
                eventId: requestEndEventId,
                eventSequence: requestEndEventSequence,
                disposition: "created" as const,
            }];
        const drafts = envelope.drafts ?? [];
        const primaryDrafts = drafts.filter((draft) => draft.draftKind !== "cancellation");
        const cancellationDrafts = drafts.filter((draft) => draft.draftKind === "cancellation");
        const messages = primaryDrafts.length === 0
            ? []
            : envelope.requestKind === "compaction_summary"
                ? (() => {
                    this.durableSequence.eventSequence += 1;
                    this.durableSequence.messageSequence = envelope.compactedThroughMessageSequence === undefined
                        ? this.durableSequence.messageSequence + 1
                        : envelope.compactedThroughMessageSequence + 1;
                    const compactionEventId = `bridge-compaction-${envelope.modelRequestId}`;
                    events.push({
                        sessionThreadId: envelope.sessionThreadId,
                        sourceEventId: envelope.modelRequestId,
                        eventId: compactionEventId,
                        eventSequence: this.durableSequence.eventSequence,
                        disposition: "created" as const,
                    });
                    return primaryDrafts.map((draft) => ({
                        runtimeLocalId: draft.runtimeLocalId,
                        sessionThreadId: envelope.sessionThreadId,
                        owningEventId: compactionEventId,
                        messageId: `message-${envelope.sessionThreadId}-${this.durableSequence.messageSequence}`,
                        messageSequence: this.durableSequence.messageSequence,
                        createdAt,
                        updatedAt: createdAt,
                        disposition: "created" as const,
                        parts: draft.parts.map((part, partIndex) => ({
                            runtimeLocalPartId: part.runtimeLocalPartId,
                            partId: `part-${envelope.sessionThreadId}-${this.durableSequence.messageSequence}-${partIndex}`,
                            messageId: `message-${envelope.sessionThreadId}-${this.durableSequence.messageSequence}`,
                            partSequence: partIndex,
                            createdAt,
                            updatedAt: createdAt,
                            disposition: "created" as const,
                        })),
                    }));
                })()
                : (() => {
                    const current = this.activeMessages.get(envelope.sessionThreadId);
                    const message = current ?? {
                        messageId: `message-${envelope.sessionThreadId}-${this.durableSequence.messageSequence + 1}`,
                        messageSequence: this.durableSequence.messageSequence + 1,
                        owningEventId: requestEndEventId,
                        parts: new Map<string, {
                            readonly partId: string;
                            readonly createdAt: string;
                        }>(),
                    };
                    if (current === undefined) {
                        this.durableSequence.messageSequence += 1;
                        this.activeMessages.set(envelope.sessionThreadId, message);
                    }
                    return primaryDrafts.map((draft) => ({
                        runtimeLocalId: draft.runtimeLocalId,
                        sessionThreadId: envelope.sessionThreadId,
                        owningEventId: message.owningEventId,
                        messageId: message.messageId,
                        messageSequence: message.messageSequence,
                        createdAt,
                        updatedAt: createdAt,
                        disposition: current === undefined ? "created" as const : "updated" as const,
                        parts: draft.parts.map((part, partIndex) => {
                            const association = testPartAssociation(part);
                            const existing = message.parts.get(association);
                            const durablePart = existing ?? {
                                partId: `part-${message.messageId}-${message.parts.size + 1}`,
                                createdAt,
                            };
                            message.parts.set(association, durablePart);
                            return {
                                runtimeLocalPartId: part.runtimeLocalPartId,
                                partId: durablePart.partId,
                                messageId: message.messageId,
                                partSequence: partIndex,
                                createdAt: durablePart.createdAt,
                                updatedAt: createdAt,
                                disposition: existing === undefined ? "created" as const : "updated" as const,
                            };
                        }),
                    }));
                })();
        const checkpointMessage = messages[0];
        const interruptReceipt = envelope.interruptSettlement === undefined
            ? undefined
            : (() => {
                const eventId = envelope.interruptSettlement.eventIds[0]!;
                const interruptMessages = cancellationDrafts.map((draft) => {
                    this.durableSequence.messageSequence += 1;
                    const messageId = `message-interrupt-${envelope.sessionThreadId}-${this.durableSequence.messageSequence}`;
                    return {
                        runtimeLocalId: draft.runtimeLocalId,
                        sessionThreadId: envelope.sessionThreadId,
                        owningEventId: eventId,
                        messageId,
                        messageSequence: this.durableSequence.messageSequence,
                        createdAt,
                        updatedAt: createdAt,
                        disposition: "created" as const,
                        parts: draft.parts.map((part, partIndex) => ({
                            runtimeLocalPartId: part.runtimeLocalPartId,
                            partId: `part-${messageId}-${partIndex}`,
                            messageId,
                            partSequence: partIndex,
                            createdAt,
                            updatedAt: createdAt,
                            disposition: "created" as const,
                        })),
                    };
                });
                return {
                    sessionThreadId: envelope.sessionThreadId,
                    operationKind: "commit_inputs",
                    sourceKind: "interrupt_control",
                    sourceId: envelope.interruptSettlement.runtimeInputId,
                    declarationDigest: `digest-interrupt-${envelope.interruptSettlement.runtimeInputId}`,
                    pendingAttachmentDelta: [],
                    pendingToolDelta: envelope.interruptSettlement.pendingToolCancellations.map((pending) => JSON.stringify({
                        result_event_id: eventId,
                        runtime_local_id: pending.runtimeLocalId,
                        status: "cancelled",
                        tool_use_event_id: pending.toolUseEventId,
                    })),
                    prefixConsumptions: [],
                    childLifecycle: [],
                    events: [{
                            sessionThreadId: envelope.sessionThreadId,
                            sourceEventId: eventId,
                            eventId,
                            eventSequence: envelope.interruptSettlement.sequenceFrom,
                            disposition: "existing" as const,
                        }],
                    messages: interruptMessages,
                };
            })();
        return {
            ...result,
            declaration: {
                applicationDisposition: "current_custody",
                observedBindingId: envelope.bindingId,
                observedBindingGeneration: envelope.bindingGeneration,
                receipt: {
                    sessionThreadId: envelope.sessionThreadId,
                    operationKind: "write_request_end",
                    sourceKind: "model_request",
                    sourceId: envelope.modelRequestId,
                    declarationDigest: `digest-${envelope.modelRequestId}`,
                    pendingAttachmentDelta: [],
                    pendingToolDelta: [],
                    prefixConsumptions: envelope.prefixConsumption === undefined || checkpointMessage === undefined
                        ? []
                        : [{
                                childThreadId: envelope.prefixConsumption.childThreadId,
                                parentBoundaryEventId: envelope.prefixConsumption.parentBoundaryEventId,
                                checkpointMessageId: checkpointMessage.messageId,
                                disposition: "consumed",
                            }],
                    childLifecycle: [],
                    events,
                    messages,
                    ...(envelope.reschedule === undefined
                        ? {}
                        : {
                            requestReschedule: {
                                disposition: "accepted",
                                requestKind: envelope.requestKind ?? "agent_provider_request",
                                attempt: envelope.reschedule.attempt,
                                effectiveDeadline: envelope.reschedule.deadline,
                            },
                        }),
                    ...(envelope.compactedThroughMessageSequence === undefined
                        ? {}
                        : { compactedThroughMessageSequence: envelope.compactedThroughMessageSequence }),
                },
                ...(interruptReceipt === undefined ? {} : { relatedReceipts: [interruptReceipt] }),
            },
            ...(envelope.reschedule === undefined
                ? {}
                : {
                    rescheduleDisposition: {
                        status: "accepted",
                        attempt: envelope.reschedule.attempt,
                        effectiveDeadline: envelope.reschedule.deadline,
                    },
                }),
        };
    }
    applyFinishIdle(envelope: SessionEventWriterFinishIdleEnvelope, result: SessionEventWriterAppendResult): SessionEventWriterAppendResult {
        if (!result.ok || result.declaration !== undefined) {
            return result;
        }
        this.durableSequence.eventSequence += 1;
        const idleEvent = {
            sessionThreadId: envelope.sessionThreadId,
            sourceEventId: envelope.durableTurnId,
            eventId: result.eventId,
            eventSequence: this.durableSequence.eventSequence,
            disposition: "created" as const,
        };
        const events = [idleEvent];
        const drafts = envelope.drafts ?? [];
        const messages = drafts.map((draft) => {
            this.durableSequence.eventSequence += 1;
            const completionEventId = `bridge-completion-${envelope.durableTurnId}`;
            events.push({
                sessionThreadId: envelope.sessionThreadId,
                sourceEventId: `delivery-${envelope.durableTurnId}`,
                eventId: completionEventId,
                eventSequence: this.durableSequence.eventSequence,
                disposition: "created" as const,
            });
            const messageId = `message-completion-${envelope.durableTurnId}`;
            return {
                runtimeLocalId: draft.runtimeLocalId,
                sessionThreadId: envelope.sessionThreadId,
                owningEventId: completionEventId,
                messageId,
                messageSequence: 0,
                createdAt,
                updatedAt: createdAt,
                disposition: "created" as const,
                parts: draft.parts.map((part, partIndex) => ({
                    runtimeLocalPartId: part.runtimeLocalPartId,
                    partId: `part-completion-${envelope.durableTurnId}-${partIndex}`,
                    messageId,
                    partSequence: partIndex,
                    createdAt,
                    updatedAt: createdAt,
                    disposition: "created" as const,
                })),
            };
        });
        return {
            ...result,
            declaration: {
                applicationDisposition: "current_custody",
                observedBindingId: envelope.bindingId,
                observedBindingGeneration: envelope.bindingGeneration,
                receipt: {
                    sessionThreadId: envelope.sessionThreadId,
                    operationKind: "finish_idle",
                    sourceKind: "turn_closeout",
                    sourceId: envelope.durableTurnId,
                    declarationDigest: `digest-finish-${envelope.durableTurnId}`,
                    pendingAttachmentDelta: [],
                    pendingToolDelta: [],
                    prefixConsumptions: [],
                    childLifecycle: [],
                    events,
                    messages,
                    idleCloseout: {
                        durableTurnId: envelope.durableTurnId,
                        idleEventId: result.eventId,
                        idleEventSequence: idleEvent.eventSequence,
                        committedIdleAt: createdAt,
                    },
                },
            },
        };
    }
    applyRuntimeTermination(envelope: SessionEventWriterRuntimeTerminationEnvelope, result: SessionEventWriterAppendResult): SessionEventWriterAppendResult {
        if (!result.ok || result.declaration !== undefined) {
            return result;
        }
        const events = ["failure", "idle"].map((kind) => {
            this.durableSequence.eventSequence += 1;
            return {
                sessionThreadId: envelope.sessionThreadId,
                sourceEventId: envelope.writeId,
                eventId: `bridge-termination-${kind}-${envelope.writeId}`,
                eventSequence: this.durableSequence.eventSequence,
                disposition: "created" as const,
            };
        });
        const messageEvents = envelope.drafts.map((_draft, index) => {
            this.durableSequence.eventSequence += 1;
            return {
                sessionThreadId: envelope.sessionThreadId,
                sourceEventId: envelope.writeId,
                eventId: `bridge-termination-${envelope.writeId}-${index}`,
                eventSequence: this.durableSequence.eventSequence,
                disposition: "created" as const,
            };
        });
        events.push(...messageEvents);
        const messages = envelope.drafts.map((draft, index) => {
            this.durableSequence.messageSequence += 1;
            const messageId = `message-termination-${envelope.writeId}-${index}`;
            return {
                runtimeLocalId: draft.runtimeLocalId,
                sessionThreadId: envelope.sessionThreadId,
                owningEventId: messageEvents[index]!.eventId,
                messageId,
                messageSequence: this.durableSequence.messageSequence,
                createdAt,
                updatedAt: createdAt,
                disposition: "created" as const,
                parts: draft.parts.map((part, partIndex) => ({
                    runtimeLocalPartId: part.runtimeLocalPartId,
                    partId: `part-termination-${envelope.writeId}-${index}-${partIndex}`,
                    messageId,
                    partSequence: partIndex,
                    createdAt,
                    updatedAt: createdAt,
                    disposition: "created" as const,
                })),
            };
        });
        return {
            ...result,
            declaration: {
                applicationDisposition: "current_custody",
                observedBindingId: envelope.bindingId,
                observedBindingGeneration: envelope.bindingGeneration,
                receipt: {
                    sessionThreadId: envelope.sessionThreadId,
                    operationKind: "commit_runtime_termination",
                    sourceKind: "runtime_termination",
                    sourceId: envelope.writeId,
                    declarationDigest: `digest-termination-${envelope.writeId}`,
                    pendingAttachmentDelta: [],
                    pendingToolDelta: envelope.pendingToolCancellations.map((pending) => JSON.stringify({
                        status: "cancelled",
                        tool_use_event_id: pending.toolUseEventId,
                    })),
                    prefixConsumptions: [],
                    childLifecycle: [],
                    events,
                    messages,
                },
            },
        };
    }
}

function withFinishIdleReceiptForTest(envelope: SessionEventWriterFinishIdleEnvelope, result: SessionEventWriterAppendResult): SessionEventWriterAppendResult {
    return new TestRuntimeDeclarationReceipts().applyFinishIdle(envelope, result);
}

function withRuntimeTerminationReceiptForTest(envelope: SessionEventWriterRuntimeTerminationEnvelope, result: SessionEventWriterAppendResult): SessionEventWriterAppendResult {
    return new TestRuntimeDeclarationReceipts().applyRuntimeTermination(envelope, result);
}

function testPartAssociation(part: RuntimeMessageDraft["parts"][number]): string {
    if (part.type === "tool") {
        return `tool:${part.toolCallId}`;
    }
    if (part.type === "reasoning") {
        return `reasoning:${part.providerPartId ?? part.ordinal}`;
    }
    return `${part.type}:${part.ordinal}`;
}

function writerFrom(append: (envelope: SessionEventEnvelope) => SessionEventWriterAppendResult, writeRequestEnd?: SessionEventWriter["writeRequestEnd"], existingMessages: readonly {
    readonly sessionThreadId: string;
    readonly message: DurableRuntimeMessage;
}[] = [], durableSequence?: TestDurableSequence): SessionEventWriter {
    const receipts = new TestRuntimeDeclarationReceipts(durableSequence);
    for (const existing of existingMessages) {
        receipts.seedMessage(existing.sessionThreadId, existing.message);
    }
    const appendWithReceipt = async (envelope: SessionEventEnvelope): Promise<SessionEventWriterAppendResult> => receipts.apply(envelope, append(envelope));
    const writeRequestEndWithReceipt = async (envelope: SessionEventWriterRequestEndEnvelope): Promise<SessionEventWriterAppendResult> => {
        const result = writeRequestEnd === undefined
            ? append({
                requestId: envelope.requestId,
                workspaceId: envelope.workspaceId,
                sessionId: envelope.sessionId,
                sessionThreadId: envelope.sessionThreadId,
                bindingId: envelope.bindingId,
                bindingGeneration: envelope.bindingGeneration,
                targetPodUid: envelope.targetPodUid,
                writeId: envelope.writeId,
                event: {
                    type: "span.model_request_end",
                    model_request_start_id: envelope.modelRequestStartEventId,
                    is_error: envelope.isError,
                    ...(envelope.errorKind !== undefined ? { error_kind: envelope.errorKind } : {}),
                    model_usage: {
                        input_tokens: envelope.usage?.inputTokens ?? 0,
                        output_tokens: envelope.usage?.outputTokens ?? 0,
                        cache_creation_input_tokens: envelope.usage?.cacheWriteTokens ?? 0,
                        cache_read_input_tokens: envelope.usage?.cacheReadTokens ?? 0,
                        speed: null,
                    },
                },
                drafts: [],
            })
            : await writeRequestEnd(envelope);
        return receipts.applyRequestEnd(envelope, result);
    };
    return {
        append: appendWithReceipt,
        writeRequestEnd: writeRequestEndWithReceipt,
        finishIdle: async (envelope) => receipts.applyFinishIdle(envelope, append({
            requestId: envelope.durableTurnId,
            workspaceId: envelope.workspaceId,
            sessionId: envelope.sessionId,
            sessionThreadId: envelope.sessionThreadId,
            bindingId: envelope.bindingId,
            bindingGeneration: envelope.bindingGeneration,
            targetPodUid: envelope.targetPodUid,
            writeId: envelope.durableTurnId,
            event: { type: "session.status_idle", stop_reason: envelope.stopReason },
            drafts: envelope.drafts ?? [],
        })),
        commitRuntimeTermination: async (envelope) => receipts.applyRuntimeTermination(envelope, {
            ok: true,
            writeId: envelope.writeId,
            eventId: envelope.writeId,
            processedAt: createdAt,
        }),
    };
}

function catalogForTest(tool: {
    readonly name: string;
    readonly description: string;
    readonly inputSchema: unknown;
    readonly permissionPolicy?: "always_allow" | "always_ask";
}): ToolCatalog {
    return {
        entries: [
            {
                name: tool.name,
                definition: tool,
                route: { kind: "gateway", operation: "RunWeb" },
                formatter: {
                    successShape: "test success text",
                    errorShape: "test error text",
                    forbiddenFields: ["route", "credentials", "bindingId"],
                },
                defaultPermissionPolicy: "always_allow",
                required: false,
            },
        ],
        configs: [{ name: tool.name, enabled: true, permissionPolicy: tool.permissionPolicy ?? "always_allow" }],
    };
}

function memoryCatalogForTest(): ToolCatalog {
    return {
        entries: [{
                name: "memory",
                definition: { name: "memory", description: "Memory", inputSchema: { type: "object" } },
                route: { kind: "bridge", operation: "RunMemory" },
                formatter: {
                    successShape: "memory success text",
                    errorShape: "memory error text",
                    forbiddenFields: ["route", "credentials", "bindingId"],
                },
                defaultPermissionPolicy: "always_allow",
                required: true,
            }],
        configs: [{ name: "memory", enabled: true, permissionPolicy: "always_allow" }],
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

function recordCompactionHint(session: ThreadRuntime, usage: RuntimeUsage): void {
    const { totalTokens: _ignoredTotal, ...usageWithoutTotal } = usage;
    session.state.recordLastRequestCompletion({
        ...usageWithoutTotal,
        inputTokens: 96000,
    }, {
        contextWindowTokens: 100000,
        outputTokenLimit: 4096,
    }, -1);
}

function compactionHistory(label: string): string {
    return `${label}\n${"historical context ".repeat(2200)}`;
}

function compactionTransportHistory(label: string): string {
    const marker = "\nRECENT_SENTINEL";
    const trailing = `${"R".repeat(31999 - marker.length)}${marker}`;
    return `${label}\n${"H".repeat(48000)}😀${trailing}`;
}

function utf8RoundTrip(value: string): string {
    return new TextDecoder("utf-8", { fatal: true }).decode(new TextEncoder().encode(value));
}

async function waitForReleaseOrAbort(release: Promise<void>, signal: AbortSignal): Promise<void> {
    if (signal.aborted) {
        throw new DOMException("aborted", "AbortError");
    }
    await new Promise<void>((resolve, reject) => {
        const onAbort = (): void => {
            reject(new DOMException("aborted", "AbortError"));
        };
        signal.addEventListener("abort", onAbort, { once: true });
        release.then(() => {
            signal.removeEventListener("abort", onAbort);
            resolve();
        }, (error) => {
            signal.removeEventListener("abort", onAbort);
            reject(error);
        });
    });
}

async function activeCompactionRun(session: ThreadRuntime = new ThreadRuntime("sesn_1"), metrics?: RuntimeMetricsSink) {
    recordCompactionHint(session, {
        inputTokens: 200,
        outputTokens: 75,
        reasoningTokens: 0,
        cacheReadTokens: 0,
        cacheWriteTokens: 0,
    });
    const loader = new RecordingContextLoader([], {
        type: "messages",
        messages: [userMessage("user-1", 0, compactionHistory("please continue"))],
    });
    const providerRelease = deferred<void>();
    const streamStarted = deferred<void>();
    const requestEndStarted = deferred<void>();
    const requestEndAck = deferred<SessionEventWriterAppendResult>();
    const requests: LLMRequest[] = [];
    const appended: SessionEvent[] = [];
    const requestEndEnvelopes: SessionEventWriterRequestEndEnvelope[] = [];
    let compactionStartEventId: string | undefined;
    let observedAbortSignal: AbortSignal | undefined;
    const llm: LLMServiceInterface = {
        stream(request, options) {
            requests.push(request);
            observedAbortSignal = options?.abortSignal;
            streamStarted.resolve();
            return Stream.fromAsyncIterable((async function* () {
                if (options?.abortSignal === undefined) {
                    throw new Error("compaction stream requires an abort signal");
                }
                await waitForReleaseOrAbort(providerRelease.promise, options.abortSignal);
                yield { type: "finish" as const, finishReason: "stop" as const };
            })(), (error): LLMServiceError => ({
                type: "llm-service",
                error: runtimeFailureFromProviderError(normalizeProviderError({
                    code: "provider_stream_error",
                    message: String(error),
                    retryable: true,
                })),
            }));
        },
    };
    const writer = writerFrom((envelope) => {
        appended.push(envelope.event);
        const eventId = `bridge-${envelope.writeId}`;
        if (envelope.event.type === "span.model_request_start") {
            compactionStartEventId = eventId;
        }
        return { ok: true, writeId: envelope.writeId, eventId, processedAt: createdAt };
    }, async (envelope) => {
        requestEndEnvelopes.push(envelope);
        requestEndStarted.resolve();
        return requestEndAck.promise;
    });
    const runFiber = Effect.runFork(Effect.gen(function* () {
        const threadLoop = yield* ThreadLoop.Service;
        return yield* threadLoop.run(session, testRunCustody());
    }).pipe(Effect.provide(runtimeThreadLoopLayer(loader, {
        llmService: llm,
        writer,
        compaction: {},
        ...(metrics === undefined ? {} : { metrics }),
    }))));
    await streamStarted.promise;
    return {
        session,
        providerRelease,
        requestEndStarted,
        requestEndAck,
        requests,
        appended,
        requestEndEnvelopes,
        compactionStartEventId,
        observedAbortSignal,
        runFiber,
    };
}

async function waitForCondition(predicate: () => boolean, label: string): Promise<void> {
    for (let attempt = 0; attempt < 100; attempt += 1) {
        if (predicate()) {
            return;
        }
        await new Promise<void>((resolve) => setImmediate(resolve));
    }
    throw new Error(`timed out waiting for ${label}`);
}

async function flushMicrotasks(count = 20): Promise<void> {
    for (let index = 0; index < count; index += 1) {
        await Promise.resolve();
    }
}

function failingEventWriter(appendedTypes: string[], shouldFail: (event: SessionEvent) => boolean): SessionEventWriter {
    return writerFrom((envelope) => {
        appendedTypes.push(envelope.event.type);
        if (!shouldFail(envelope.event)) {
            return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
        }
        return {
            ok: false,
            error: {
                type: "session-event-writer",
                code: "unavailable",
                message: "append failed",
                retryable: false,
                fatal: false,
                sessionId: envelope.sessionId,
                writeId: envelope.writeId,
            },
        };
    });
}

function runtimeThreadLoopLayer(loader: TestContextLoader, options: {
    readonly events?: readonly LLMEvent[];
    readonly store?: ThreadLoopRuntimeStore;
    readonly writer?: SessionEventWriter;
    readonly llmService?: LLMServiceInterface;
    readonly onStream?: (request: LLMRequest) => void;
    readonly createProcessor?: Parameters<typeof ThreadLoop.layer>[0]["createProcessor"];
    readonly providerCallRuntime?: ProviderCallRuntimeConfig;
    readonly providerCallAssembler?: ProviderCallAssembler;
    readonly compaction?: Parameters<typeof ThreadLoop.layer>[0]["compaction"];
    readonly approvalMode?: Parameters<typeof ThreadLoop.layer>[0]["approvalMode"];
    readonly runTool?: Parameters<typeof ThreadLoop.layer>[0]["runTool"];
    readonly acceptSandboxExecution?: Parameters<typeof ThreadLoop.layer>[0]["acceptSandboxExecution"];
    readonly awaitSandboxExecution?: Parameters<typeof ThreadLoop.layer>[0]["awaitSandboxExecution"];
    readonly reviewApproval?: Parameters<typeof ThreadLoop.layer>[0]["reviewApproval"];
    readonly runtimeModel?: Parameters<typeof ThreadLoop.layer>[0]["runtimeModel"];
    readonly runtimePolicy?: Parameters<typeof ThreadLoop.layer>[0]["runtimePolicy"];
    readonly runtime?: RuntimeDependencies;
    readonly metrics?: RuntimeMetricsSink;
    readonly refreshRuntimeBindingToken?: Parameters<typeof ThreadLoop.layer>[0]["refreshRuntimeBindingToken"];
    readonly installLoaderState?: boolean;
} = {}): Layer.Layer<ThreadLoop.Service> {
    const order: string[] = [];
    const store = options.store ?? new ThreadLoopRuntimeStore(order);
    const writer = options.writer ?? writerFrom((envelope) => ({
        ok: true,
        writeId: envelope.writeId,
        eventId: `bridge-${envelope.writeId}`,
        processedAt: createdAt,
    }));
    const productionLayer = ThreadLoop.layer({
        internalToolRepairStore: store,
        sessionEventWriter: writer,
        runtime: options.runtime ?? threadLoopRuntime(),
        llmService: options.llmService ?? (options.events === undefined
            ? llmService([
                { type: "text-start", id: "text-1" },
                { type: "text-delta", id: "text-1", text_delta: "ok" },
                { type: "text-end", id: "text-1" },
                { type: "finish", finishReason: "stop" },
            ], options.onStream)
            : firstRequestThenFinalResponse(options.events, options.onStream)),
        storeOperationTimeoutMs: 1000,
        ...(options.createProcessor !== undefined ? { createProcessor: options.createProcessor } : {}),
        providerCallRuntime: {
            ...DefaultProviderCallRuntimeConfig,
            timeoutMs: 1800000,
            ...options.providerCallRuntime,
            approvalReviewerPolicy: options.providerCallRuntime?.approvalReviewerPolicy ?? approvalReviewerPolicy,
        },
        ...(options.providerCallAssembler !== undefined ? { providerCallAssembler: options.providerCallAssembler } : {}),
        ...(options.compaction !== undefined ? { compaction: { timeoutMs: 1800000, ...options.compaction } } : {}),
        ...(options.approvalMode !== undefined ? { approvalMode: options.approvalMode } : {}),
        ...(options.runTool !== undefined ? { runTool: options.runTool } : {}),
        acceptSandboxExecution: options.acceptSandboxExecution ?? (() => ({ type: "accepted" as const })),
        ...(options.awaitSandboxExecution !== undefined
            ? { awaitSandboxExecution: options.awaitSandboxExecution }
            : options.runTool !== undefined
                ? { awaitSandboxExecution: options.runTool }
                : {}),
        ...(options.reviewApproval !== undefined ? { reviewApproval: options.reviewApproval } : {}),
        runtimeModel: options.runtimeModel ?? (() => ({ providerId: "fake", modelId: "fake-chat" })),
        runtimePolicy: options.runtimePolicy ?? (() => ({
            toolCatalog: options.providerCallRuntime?.toolCatalog ?? createToolCatalog({ family: "claude" }),
        })),
        ...(options.metrics !== undefined ? { metrics: options.metrics } : {}),
        ...(options.refreshRuntimeBindingToken !== undefined ? { refreshRuntimeBindingToken: options.refreshRuntimeBindingToken } : {}),
    }).pipe(Layer.provide(ThreadLoop.contextLoaderLayer(loader)));
    if (options.installLoaderState === false) {
        return productionLayer;
    }
    return Layer.effect(ThreadLoop.Service, Effect.gen(function* () {
        const production = yield* ThreadLoop.Service;
        return ThreadLoop.Service.of({
            ...production,
            run: (session, custody) => Effect.promise(() => installLoaderStateForTest(loader, session)).pipe(Effect.flatMap(() => production.run(session, custody))),
        });
    }).pipe(Effect.provide(productionLayer)));
}

class RecordingRuntimeMetrics implements RuntimeMetricsSink {
    readonly providerStreamDurations: Array<{
        readonly kind: RuntimeProviderStreamKind;
        readonly durationMs: number;
        readonly outcome: RuntimeMetricOutcome;
    }> = [];
    readonly eventWriteLatencies: Array<{
        readonly operation: RuntimeEventWriteOperation;
        readonly durationMs: number;
        readonly outcome: RuntimeMetricOutcome;
    }> = [];
    readonly contextLoadLatencies: Array<{
        readonly operation: RuntimeContextLoadOperation;
        readonly durationMs: number;
        readonly outcome: RuntimeMetricOutcome;
    }> = [];
    recordHotState(_snapshot: RuntimeHotStateMetrics): void { }
    addActiveToolFibers(): void { }
    addPendingApprovals(): void { }
    observeProviderStreamDuration(kind: RuntimeProviderStreamKind, durationMs: number, outcome: RuntimeMetricOutcome): void {
        this.providerStreamDurations.push({ kind, durationMs, outcome });
    }
    observeEventWriteLatency(operation: RuntimeEventWriteOperation, durationMs: number, outcome: RuntimeMetricOutcome): void {
        this.eventWriteLatencies.push({ operation, durationMs, outcome });
    }
    observeContextLoadLatency(operation: RuntimeContextLoadOperation, durationMs: number, outcome: RuntimeMetricOutcome): void {
        this.contextLoadLatencies.push({ operation, durationMs, outcome });
    }
    recordCleanupCommandOutcome(): void { }
}

export {
  ProviderDiagnosticCanaries,
  QueuedContextLoader,
  RecordingContextLoader,
  RecordingRuntimeMetrics,
  TestRuntimeDeclarationReceipts,
  ThreadLoopRuntimeStore,
  acceptedInput,
  activeCompactionRun,
  approvalReviewAcceptedInput,
  approvalReviewerOutputSchemaJson,
  approvalReviewerPolicy,
  beginTestUserInterrupt,
  catalogForTest,
  compactionHistory,
  compactionTransportHistory,
  createdAt,
  deferred,
  emptyColdCoverage,
  expectNoProviderDiagnosticCanaries,
  failingEventWriter,
  firstRequestThenFinalResponse,
  flushMicrotasks,
  installLoaderStateForTest,
  installPendingInputForTest,
  installRecoveredToolTurn,
  llmService,
  memoryCatalogForTest,
  queuedLLMService,
  recordCompactionHint,
  runtimeThreadLoopLayer,
  sleepUntilAborted,
  testAcceptedInputSequence,
  testControlCommit,
  testPartAssociation,
  testRunCustody,
  threadLoopRuntime,
  utf8RoundTrip,
  waitForCondition,
  waitForReleaseOrAbort,
  withFinishIdleReceiptForTest,
  withRuntimeTerminationReceiptForTest,
  writerFrom,
};

export type {
  PackageJson,
  TestContextLoader,
  TestDurableSequence,
};
