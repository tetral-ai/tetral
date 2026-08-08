import { expect } from "bun:test";
import { readFile } from "node:fs/promises";
import { Cause, Context, Effect, Exit, Fiber, Layer, Scope, Stream } from "effect";
import { ProviderRequestKind, RuntimeMessageRole, SystemCacheHint, SystemSegmentKind, } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { DurableRuntimeMessage, PendingInputResult, RuntimeDeclarationReceipt, RuntimeDependencies, RuntimeInternalToolRepairCommit, RuntimeMessage, RuntimeMessageInfo, RuntimeDeclarationOperationControls, RuntimePart, RuntimePartCreate, SessionEvent, SessionEventEnvelope, SessionEventWriter, SessionEventWriterAppendResult, SessionEventWriterFinishIdleEnvelope, SessionEventWriterRequestEndEnvelope, SessionEventWriterRuntimeTerminationEnvelope, } from "../../../src/contracts/runtime.js";
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
        const part = repair.messageCreate.parts[0];
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
					operationId: repair.repairKey,
                    declarationDigest: "test-repair-digest",
                    pendingAttachmentDelta: [],
                    events: [{
                            sessionThreadId: repair.sessionThreadId,
                            eventId,
                            eventSequence,
                            disposition: "created",
                        }],
                    messages: [{
                            sessionThreadId: repair.sessionThreadId,
                            owningEventId: eventId,
                            messageId,
                            messageSequence,
                            createdAt,
                            updatedAt: createdAt,
                            disposition: "created",
                            parts: [{
                                    partId,
                                    messageId,
                                    partSequence: 0,
                                    createdAt,
                                    updatedAt: createdAt,
                                    disposition: "created",
                                }],
						}],
					interruptToolProjections: [],
					prefixConsumptions: [],
					childLifecycle: [],
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

interface TestAssistantProjection {
    readonly messageId: string;
    readonly messageSequence: number;
    readonly owningEventId: string;
    readonly owningEventSequence: number;
    nextPartSequence: number;
    unfinishedTools: Array<{ readonly toolUseEventId: string; readonly partSequence: number }>;
}

class TestRuntimeDeclarationReceipts {
    private readonly sequence: TestDurableSequence;
    private readonly assistants = new Map<string, TestAssistantProjection>();

    constructor(sequence: TestDurableSequence = { eventSequence: 0, messageSequence: 0 }) {
        this.sequence = sequence;
    }

    seedMessage(sessionThreadId: string, message: DurableRuntimeMessage): void {
        this.sequence.eventSequence = Math.max(this.sequence.eventSequence, message.eventSequence);
        this.sequence.messageSequence = Math.max(this.sequence.messageSequence, message.sequence);
        if (message.role !== "assistant") return;
        const prior = this.assistants.get(sessionThreadId);
        if (prior !== undefined && prior.messageSequence > message.sequence) return;
        this.assistants.set(sessionThreadId, {
            messageId: message.id,
            messageSequence: message.sequence,
            owningEventId: message.owningEventId,
            owningEventSequence: message.eventSequence,
            nextPartSequence: Math.max(-1, ...message.parts.map((part) => part.sequence)) + 1,
            unfinishedTools: message.parts
                .filter((part): part is Extract<RuntimePart, { readonly type: "tool" }> =>
                    part.type === "tool" && part.toolUseEventId !== undefined &&
                    part.state.status !== "completed" && part.state.status !== "error" && part.state.status !== "cancelled")
                .map((part) => ({ toolUseEventId: part.toolUseEventId!, partSequence: part.sequence })),
        });
    }

    apply(envelope: SessionEventEnvelope, result: SessionEventWriterAppendResult): SessionEventWriterAppendResult {
        if (!result.ok) return result;
        if (envelope.event.type === "span.model_request_start") {
            this.assistants.delete(envelope.sessionThreadId);
        }
        const event = this.eventStamp(envelope.sessionThreadId, result.eventId);
        let messages: RuntimeDeclarationReceipt["messages"] = [];
        if (envelope.assistantPartAppend !== undefined) {
            messages = [this.appendStamp(envelope.sessionThreadId, event, envelope.assistantPartAppend.parts)];
        }
        if (envelope.toolSettlement !== undefined) {
            this.settleTool(envelope.sessionThreadId, envelope.toolSettlement.toolUseEventId);
        }
        const receipt = this.receipt({
            sessionThreadId: envelope.sessionThreadId,
            operationKind: "write_event",
            sourceKind: envelope.event.type,
            operationId: envelope.writeId,
            events: [event],
            messages,
            ...(envelope.event.type === "span.model_request_start" ? {
                requestStart: {
                    requestKind: envelope.requestKind!,
                    contextThroughMessageSequence: envelope.contextThroughMessageSequence!,
                },
            } : {}),
        });
        return this.withDeclaration(envelope, result, receipt);
    }

    applyRequestEnd(envelope: SessionEventWriterRequestEndEnvelope, result: SessionEventWriterAppendResult): SessionEventWriterAppendResult {
        if (!result.ok) return result;
        const requestEnd = this.eventStamp(envelope.sessionThreadId, result.eventId);
        const events: RuntimeDeclarationReceipt["events"][number][] = [requestEnd];
        let messages: RuntimeDeclarationReceipt["messages"] = [];
        if (envelope.trailingPartAppend !== undefined) {
            messages = [this.appendStamp(envelope.sessionThreadId, requestEnd, envelope.trailingPartAppend.parts)];
        }
        if (envelope.compactionCheckpointCreate !== undefined) {
            const compactionEvent = this.eventStamp(envelope.sessionThreadId, `sevt_compaction_${this.sequence.eventSequence + 1}`);
            events.push(compactionEvent);
            const messageSequence = (envelope.compactedThroughMessageSequence ?? this.sequence.messageSequence) + 1;
            messages = [this.createStamp(
                envelope.sessionThreadId,
                compactionEvent,
                envelope.compactionCheckpointCreate.parts,
                messageSequence,
            )];
        }
        const receipt = this.receipt({
            sessionThreadId: envelope.sessionThreadId,
            operationKind: "write_request_end",
            sourceKind: "model_request",
            operationId: envelope.modelRequestId,
            events,
            messages,
            ...(envelope.prefixConsumption === undefined || messages[0] === undefined ? {} : {
                prefixConsumptions: [{
                    childThreadId: envelope.prefixConsumption.childThreadId,
                    parentBoundaryEventId: envelope.prefixConsumption.parentBoundaryEventId,
                    checkpointMessageId: messages[0].messageId,
                    disposition: "consumed" as const,
                }],
            }),
            ...(envelope.compactedThroughMessageSequence === undefined ? {} : {
                compactedThroughMessageSequence: envelope.compactedThroughMessageSequence,
            }),
            ...(envelope.reschedule === undefined ? {} : {
                requestReschedule: {
                    disposition: "accepted" as const,
                    requestKind: envelope.requestKind ?? "agent_provider_request",
                    attempt: envelope.reschedule.attempt,
                    effectiveDeadline: envelope.reschedule.deadline,
                },
            }),
        });
        const relatedReceipts = envelope.interruptSettlement === undefined
            ? undefined
            : [this.interruptReceipt(envelope, envelope.interruptSettlement)];
        return this.withDeclaration(envelope, result, receipt, relatedReceipts);
    }

    applyFinishIdle(envelope: SessionEventWriterFinishIdleEnvelope, result: SessionEventWriterAppendResult): SessionEventWriterAppendResult {
        if (!result.ok) return result;
        const idleEvent = this.eventStamp(envelope.sessionThreadId, result.eventId);
        const events: RuntimeDeclarationReceipt["events"][number][] = [idleEvent];
        let messages: RuntimeDeclarationReceipt["messages"] = [];
        if (envelope.completionMailCreate !== undefined) {
            const mailEvent = this.eventStamp(envelope.sessionThreadId, `sevt_completion_${this.sequence.eventSequence + 1}`);
            events.push(mailEvent);
            messages = [this.createStamp(envelope.sessionThreadId, mailEvent, envelope.completionMailCreate.parts)];
        }
        const receipt = this.receipt({
            sessionThreadId: envelope.sessionThreadId,
            operationKind: "finish_idle",
            sourceKind: "turn_closeout",
            operationId: envelope.durableTurnId,
            events,
            messages,
            idleCloseout: {
                durableTurnId: envelope.durableTurnId,
                idleEventId: idleEvent.eventId,
                idleEventSequence: idleEvent.eventSequence,
                committedIdleAt: result.processedAt,
            },
        });
        return this.withDeclaration(envelope, result, receipt);
    }

    applyRuntimeTermination(envelope: SessionEventWriterRuntimeTerminationEnvelope, result: SessionEventWriterAppendResult): SessionEventWriterAppendResult {
        if (!result.ok) return result;
        const events: RuntimeDeclarationReceipt["events"][number][] = [];
        for (const settlement of envelope.toolSettlements) {
            events.push(this.eventStamp(envelope.sessionThreadId, `sevt_tool_result_${this.sequence.eventSequence + 1}`));
            this.settleTool(envelope.sessionThreadId, settlement.toolUseEventId);
        }
        let messages: RuntimeDeclarationReceipt["messages"] = [];
        if (envelope.completionMailCreate !== undefined) {
            const mailEvent = this.eventStamp(envelope.sessionThreadId, `sevt_completion_${this.sequence.eventSequence + 1}`);
            events.push(mailEvent);
            messages = [this.createStamp(envelope.sessionThreadId, mailEvent, envelope.completionMailCreate.parts)];
        }
        events.push(this.eventStamp(envelope.sessionThreadId, `sevt_failure_${this.sequence.eventSequence + 1}`));
        events.push(this.eventStamp(envelope.sessionThreadId, result.eventId));
        const receipt = this.receipt({
            sessionThreadId: envelope.sessionThreadId,
            operationKind: "commit_runtime_termination",
            sourceKind: "runtime_termination",
            operationId: envelope.writeId,
            events,
            messages,
        });
        return this.withDeclaration(envelope, result, receipt);
    }

    private interruptReceipt(
        envelope: SessionEventWriterRequestEndEnvelope,
        interrupt: NonNullable<SessionEventWriterRequestEndEnvelope["interruptSettlement"]>,
    ): RuntimeDeclarationReceipt {
        const active = this.assistants.get(envelope.sessionThreadId);
        const unfinished = [...(active?.unfinishedTools ?? [])].sort((left, right) => left.partSequence - right.partSequence);
        const projections = unfinished.map(({ toolUseEventId }) => ({
            toolUseEventId,
            resultEvent: this.eventStamp(envelope.sessionThreadId, `sevt_interrupt_result_${this.sequence.eventSequence + 1}`),
            terminalState: { type: "cancelled" as const },
        }));
        if (active !== undefined) active.unfinishedTools = [];
        return this.receipt({
            sessionThreadId: envelope.sessionThreadId,
            operationKind: "commit_inputs",
            sourceKind: "interrupt_control",
            operationId: interrupt.runtimeInputId,
            events: [{
                sessionThreadId: envelope.sessionThreadId,
                eventId: interrupt.eventIds[0]!,
                eventSequence: interrupt.sequenceFrom,
                disposition: "existing",
            }],
            messages: [],
            interruptToolProjections: projections,
        });
    }

    private appendStamp(
        sessionThreadId: string,
        event: RuntimeDeclarationReceipt["events"][number],
        parts: readonly RuntimePartCreate[],
    ): RuntimeDeclarationReceipt["messages"][number] {
        let assistant = this.assistants.get(sessionThreadId);
        const created = assistant === undefined;
        if (assistant === undefined) {
            this.sequence.messageSequence += 1;
            assistant = {
                messageId: `msg_assistant_${this.sequence.messageSequence}`,
                messageSequence: this.sequence.messageSequence,
                owningEventId: event.eventId,
                owningEventSequence: event.eventSequence,
                nextPartSequence: 0,
                unfinishedTools: [],
            };
            this.assistants.set(sessionThreadId, assistant);
        }
        const partStamps = this.partStamps(assistant.messageId, assistant.nextPartSequence, parts.length);
        parts.forEach((part, index) => {
            if (part.type !== "tool") return;
            const toolUseEventId = part.toolUseEventId ??
                (index === parts.length - 1 ? event.eventId : undefined);
            if (toolUseEventId !== undefined && part.state.status !== "completed" && part.state.status !== "error" && part.state.status !== "cancelled") {
                assistant!.unfinishedTools.push({ toolUseEventId, partSequence: partStamps[index]!.partSequence });
            }
        });
        assistant.nextPartSequence += parts.length;
        return {
            sessionThreadId,
            owningEventId: assistant.owningEventId,
            messageId: assistant.messageId,
            messageSequence: assistant.messageSequence,
            createdAt,
            updatedAt: createdAt,
            disposition: created ? "created" : "updated",
            parts: partStamps,
        };
    }

    private createStamp(
        sessionThreadId: string,
        event: RuntimeDeclarationReceipt["events"][number],
        parts: readonly RuntimePartCreate[],
        forcedMessageSequence?: number,
    ): RuntimeDeclarationReceipt["messages"][number] {
        const messageSequence = forcedMessageSequence ?? this.sequence.messageSequence + 1;
        this.sequence.messageSequence = Math.max(this.sequence.messageSequence, messageSequence);
        const messageId = `msg_created_${messageSequence}`;
        return {
            sessionThreadId,
            owningEventId: event.eventId,
            messageId,
            messageSequence,
            createdAt,
            updatedAt: createdAt,
            disposition: "created",
            parts: this.partStamps(messageId, 0, parts.length),
        };
    }

    private partStamps(messageId: string, firstSequence: number, count: number): RuntimeDeclarationReceipt["messages"][number]["parts"] {
        return Array.from({ length: count }, (_, index) => ({
            partId: `part_${messageId}_${firstSequence + index}`,
            messageId,
            partSequence: firstSequence + index,
            createdAt,
            updatedAt: createdAt,
            disposition: "created" as const,
        }));
    }

    private eventStamp(sessionThreadId: string, eventId: string): RuntimeDeclarationReceipt["events"][number] {
        this.sequence.eventSequence += 1;
        return {
            sessionThreadId,
            eventId,
            eventSequence: this.sequence.eventSequence,
            disposition: "created",
        };
    }

    private settleTool(sessionThreadId: string, toolUseEventId: string): void {
        const active = this.assistants.get(sessionThreadId);
        if (active === undefined) return;
        active.unfinishedTools = active.unfinishedTools.filter((tool) => tool.toolUseEventId !== toolUseEventId);
    }

    private receipt(input: {
        readonly sessionThreadId: string;
        readonly operationKind: string;
        readonly sourceKind: string;
        readonly operationId: string;
        readonly events: RuntimeDeclarationReceipt["events"];
        readonly messages: RuntimeDeclarationReceipt["messages"];
        readonly interruptToolProjections?: RuntimeDeclarationReceipt["interruptToolProjections"];
        readonly prefixConsumptions?: RuntimeDeclarationReceipt["prefixConsumptions"];
        readonly requestReschedule?: RuntimeDeclarationReceipt["requestReschedule"];
        readonly requestStart?: RuntimeDeclarationReceipt["requestStart"];
        readonly idleCloseout?: RuntimeDeclarationReceipt["idleCloseout"];
        readonly compactedThroughMessageSequence?: number;
    }): RuntimeDeclarationReceipt {
        return {
            sessionThreadId: input.sessionThreadId,
            operationKind: input.operationKind,
            sourceKind: input.sourceKind,
            operationId: input.operationId,
            declarationDigest: `digest_${input.operationId}`,
            events: input.events,
            messages: input.messages,
            pendingAttachmentDelta: [],
            interruptToolProjections: input.interruptToolProjections ?? [],
            prefixConsumptions: input.prefixConsumptions ?? [],
            childLifecycle: [],
            ...(input.requestReschedule === undefined ? {} : { requestReschedule: input.requestReschedule }),
            ...(input.requestStart === undefined ? {} : { requestStart: input.requestStart }),
            ...(input.idleCloseout === undefined ? {} : { idleCloseout: input.idleCloseout }),
            ...(input.compactedThroughMessageSequence === undefined ? {} : {
                compactedThroughMessageSequence: input.compactedThroughMessageSequence,
            }),
        };
    }

    private withDeclaration(
        envelope: { readonly bindingId: string; readonly bindingGeneration: number },
        result: Extract<SessionEventWriterAppendResult, { readonly ok: true }>,
        receipt: RuntimeDeclarationReceipt,
        relatedReceipts?: readonly RuntimeDeclarationReceipt[],
    ): SessionEventWriterAppendResult {
        const reschedule = receipt.requestReschedule;
        return {
            ...result,
            declaration: {
                receipt,
                ...(relatedReceipts === undefined ? {} : { relatedReceipts: [...relatedReceipts] }),
                applicationDisposition: "current_custody",
                observedBindingId: envelope.bindingId,
                observedBindingGeneration: envelope.bindingGeneration,
            },
            ...(reschedule === undefined ? {} : {
                rescheduleDisposition: reschedule.disposition === "accepted"
                    ? {
                        status: "accepted" as const,
                        attempt: reschedule.attempt,
                        effectiveDeadline: reschedule.effectiveDeadline,
                    }
                    : {
                        status: "denied" as const,
                        reason: reschedule.disposition === "denied_attempt_mismatch"
                            ? "attempt_mismatch" as const
                            : "budget_exhausted" as const,
                        attempt: reschedule.attempt,
                    },
            }),
        };
    }
}

function withFinishIdleReceiptForTest(envelope: SessionEventWriterFinishIdleEnvelope, result: SessionEventWriterAppendResult): SessionEventWriterAppendResult {
    return new TestRuntimeDeclarationReceipts().applyFinishIdle(envelope, result);
}

function withRuntimeTerminationReceiptForTest(envelope: SessionEventWriterRuntimeTerminationEnvelope, result: SessionEventWriterAppendResult): SessionEventWriterAppendResult {
    return new TestRuntimeDeclarationReceipts().applyRuntimeTermination(envelope, result);
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
        })),
        commitRuntimeTermination: async (envelope) => receipts.applyRuntimeTermination(envelope, {
            ok: true,
            writeId: envelope.writeId,
            eventId: `sevt_termination_${envelope.writeId}`,
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
