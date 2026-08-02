import { describe, expect, jest, test } from "bun:test";
import { readFile } from "node:fs/promises";
import { Cause, Context, Effect, Exit, Fiber, Layer, Scope, Stream } from "effect";
import {
  ProviderRequestKind,
  RuntimeMessageRole,
  SystemCacheHint,
  SystemSegmentKind,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type {
  DurableRuntimeMessage,
  PendingInputResult,
  RuntimeDependencies,
  RuntimeInternalToolRepairCommit,
  RuntimeMessage,
  RuntimeMessageDraft,
  RuntimeMessageInfo,
  RuntimeDeclarationOperationControls,
  RuntimePart,
  SessionEvent,
  SessionEventEnvelope,
  SessionEventWriter,
  SessionEventWriterAppendResult,
  SessionEventWriterFinishIdleEnvelope,
  SessionEventWriterRequestEndEnvelope,
  SessionEventWriterRuntimeTerminationEnvelope,
} from "../../src/contracts/runtime.js";
import {
  DurableRuntimeMessageSchema,
  RuntimeMessageSchema,
  RuntimeInternalToolRepairStore,
  SessionEventWriterRetryPolicy,
  normalizeContextLoaderError,
  normalizeRuntimeMessageStoreError,
  normalizeSessionEventWriterError,
} from "../../src/contracts/runtime.js";
import { LLMEventSchema } from "../../src/llm/llm-event.js";
import type { LLMEvent, RuntimeUsage } from "../../src/llm/llm-event.js";
import { runtimeFailureFromProviderError } from "../../src/llm/llm-event.js";
import type { Interface as LLMServiceInterface, LLMRequest, LLMServiceError } from "../../src/llm/llm-service.js";
import type { ProviderCallAssembler, ProviderCallRuntimeConfig } from "../../src/agent-loop/provider-call-assembly.js";
import { DefaultProviderCallRuntimeConfig, assembleProviderCallRequest } from "../../src/agent-loop/provider-call-assembly.js";
import { createToolCatalog } from "../../src/tools/tool-catalog.js";
import type { ToolCatalog } from "../../src/tools/tool-catalog.js";
import * as AgentLoop from "../../src/agent-loop/agent-loop.js";
import * as SessionManager from "../../src/session/session-manager.js";
import type {
  RuntimeContextLoadOperation,
  RuntimeEventWriteOperation,
  RuntimeHotStateMetrics,
  RuntimeMetricOutcome,
  RuntimeMetricsSink,
  RuntimeProviderStreamKind,
} from "../../src/runtime/metrics.js";
import type { AcceptedInputCommitResult, ContextLoader } from "../../src/context/context-loader.js";
import { normalizeProviderError } from "../../src/contracts/provider.js";
import { Session } from "../../src/session/session.js";
import { AutoApprovalReviewerManager } from "../../src/session/approval-reviewer-manager.js";
import type {
  RuntimeAcceptedInputState,
  RuntimeConfigPatchState,
  RuntimeControlInputDeclaration,
} from "../../src/session/session-state.js";
import { SessionProcessor } from "../../src/runtime/accumulator.js";
import { SessionToolCoordinator } from "../../src/tools/tool-scheduler.js";
import { runtimeModelForThread, runtimeToolPolicyFromPatchPayloads } from "../../../runtime-pod/src/command.js";
import {
  buildAgentLoopUserMessage as userMessage,
  buildAgentLoopRuntimeNotificationMessage as runtimeNotificationMessage,
  buildRuntimeControlCommitResult,
} from "./runtime-message-builders.js";
import { acceptedInputReceipt } from "./runtime-declaration-fixtures.js";

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

function testControlCommit(
  scope: RuntimeAcceptedInputState,
  inputKind: "interrupt_control" | "tool_confirmation" = "interrupt_control",
) {
  return async (declaration: RuntimeControlInputDeclaration) =>
    buildRuntimeControlCommitResult(scope, inputKind, declaration);
}

function beginTestUserInterrupt(
  session: Session,
  runtimeInputId: string,
  onCommit: () => void = () => {},
): void {
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

function approvalReviewAcceptedInput(runtimeInputId = "rin_approval_review"): Extract<RuntimeAcceptedInputState, { readonly kind: "approval_review" }> {
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

  constructor(
    private readonly history: readonly RuntimeMessage[],
    private readonly pending: PendingInputResult,
  ) {}

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

  constructor(
    private readonly history: readonly RuntimeMessage[],
    private readonly pendingResults: PendingInputResult[],
    private readonly acceptedResults: Array<unknown | ((input: RuntimeAcceptedInputState) => unknown)> = [],
  ) {}

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

function installPendingInputForTest(session: Session, pending: PendingInputResult): void {
  if (
    session.state.peekAcceptedInput() !== undefined ||
    pending.type !== "messages" ||
    pending.messages.length === 0
  ) {
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

async function installLoaderStateForTest(loader: TestContextLoader, session: Session): Promise<void> {
  if (!session.state.persistentContextLoaded()) {
    session.state.contextManager.replaceMessages(await loader.buildContext(session.sessionId));
    session.state.markPersistentContextLoaded();
  }
  if (session.state.peekAcceptedInput() === undefined) {
    installPendingInputForTest(session, await loader.loadPendingInput(session.sessionId));
  }
}

class AgentLoopRuntimeStore extends RuntimeInternalToolRepairStore {
  readonly messages = new Map<string, RuntimeMessage>();
  readonly repairs: RuntimeInternalToolRepairCommit[] = [];

  constructor(
    private readonly order: string[],
    private readonly failPartWrite: boolean | ((part: RuntimePart) => boolean) = false,
    private readonly failMessageWrite = false,
    private readonly beforeWrite?: (operation: "writeMessage" | "writePart", payload: RuntimeMessageInfo | RuntimePart) => void,
    private readonly beforeInternalToolRepair?: (repair: RuntimeInternalToolRepairCommit) => void | Promise<void>,
    private readonly durableSequence?: TestDurableSequence,
  ) {
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
    this.messages.set(
      existing.id,
      RuntimeMessageSchema.parse({
        ...existing,
        parts: [...existing.parts.filter((current) => current.id !== part.id), part].sort((left, right) => left.sequence - right.sequence),
      }),
    );
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

function agentLoopRuntime() {
  let counter = 0;
  return {
    now: () => createdAt,
    monotonicMs: () => 0,
    createId: (prefix: string) => `${prefix}-${++counter}`,
    sleep: async () => true,
  } satisfies RuntimeDependencies;
}

function testRunCustody(initialDurableTurnId?: string): AgentLoop.AgentLoopRunCustody {
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
  constructor(
    private readonly durableSequence: TestDurableSequence = {
      eventSequence: 100_000,
      messageSequence: 100_000,
    },
  ) {}

  private readonly activeMessages = new Map<string, {
    readonly messageId: string;
    readonly messageSequence: number;
    readonly owningEventId: string;
    readonly parts: Map<string, { readonly partId: string; readonly createdAt: string }>;
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

  apply(
    envelope: SessionEventEnvelope,
    result: SessionEventWriterAppendResult,
  ): SessionEventWriterAppendResult {
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
            parts: new Map<string, { readonly partId: string; readonly createdAt: string }>(),
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

  applyRequestEnd(
    envelope: SessionEventWriterRequestEndEnvelope,
    result: SessionEventWriterAppendResult,
  ): SessionEventWriterAppendResult {
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
              parts: new Map<string, { readonly partId: string; readonly createdAt: string }>(),
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
            pendingToolDelta: envelope.interruptSettlement.pendingToolCancellations.map((pending) =>
              JSON.stringify({
                result_event_id: eventId,
                runtime_local_id: pending.runtimeLocalId,
                status: "cancelled",
                tool_use_event_id: pending.toolUseEventId,
              })
            ),
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
          prefixConsumptions:
            envelope.prefixConsumption === undefined || checkpointMessage === undefined
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

  applyFinishIdle(
    envelope: SessionEventWriterFinishIdleEnvelope,
    result: SessionEventWriterAppendResult,
  ): SessionEventWriterAppendResult {
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

  applyRuntimeTermination(
    envelope: SessionEventWriterRuntimeTerminationEnvelope,
    result: SessionEventWriterAppendResult,
  ): SessionEventWriterAppendResult {
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
          pendingToolDelta: envelope.pendingToolCancellations.map((pending) =>
            JSON.stringify({
              status: "cancelled",
              tool_use_event_id: pending.toolUseEventId,
            })
          ),
          prefixConsumptions: [],

          childLifecycle: [],
          events,
          messages,
        },
      },
    };
  }
}

function withFinishIdleReceiptForTest(
  envelope: SessionEventWriterFinishIdleEnvelope,
  result: SessionEventWriterAppendResult,
): SessionEventWriterAppendResult {
  return new TestRuntimeDeclarationReceipts().applyFinishIdle(envelope, result);
}

function withRuntimeTerminationReceiptForTest(
  envelope: SessionEventWriterRuntimeTerminationEnvelope,
  result: SessionEventWriterAppendResult,
): SessionEventWriterAppendResult {
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

function writerFrom(
  append: (envelope: SessionEventEnvelope) => SessionEventWriterAppendResult,
  writeRequestEnd?: SessionEventWriter["writeRequestEnd"],
  existingMessages: readonly {
    readonly sessionThreadId: string;
    readonly message: DurableRuntimeMessage;
  }[] = [],
  durableSequence?: TestDurableSequence,
): SessionEventWriter {
  const receipts = new TestRuntimeDeclarationReceipts(durableSequence);
  for (const existing of existingMessages) {
    receipts.seedMessage(existing.sessionThreadId, existing.message);
  }
  const appendWithReceipt = async (envelope: SessionEventEnvelope): Promise<SessionEventWriterAppendResult> =>
    receipts.apply(envelope, append(envelope));
  const writeRequestEndWithReceipt = async (
    envelope: SessionEventWriterRequestEndEnvelope,
  ): Promise<SessionEventWriterAppendResult> => {
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

function deferred<T>(): { readonly promise: Promise<T>; readonly resolve: (value: T) => void } {
  let resolve: (value: T) => void = () => undefined;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

function recordCompactionHint(session: Session, usage: RuntimeUsage): void {
  const { totalTokens: _ignoredTotal, ...usageWithoutTotal } = usage;
  session.state.recordLastRequestCompletion({
    ...usageWithoutTotal,
    inputTokens: 96_000,
  }, {
    contextWindowTokens: 100_000,
    outputTokenLimit: 4_096,
  }, -1);
}

function compactionHistory(label: string): string {
  return `${label}\n${"historical context ".repeat(2_200)}`;
}

function compactionTransportHistory(label: string): string {
  const marker = "\nRECENT_SENTINEL";
  const trailing = `${"R".repeat(31_999 - marker.length)}${marker}`;
  return `${label}\n${"H".repeat(48_000)}😀${trailing}`;
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
    release.then(
      () => {
        signal.removeEventListener("abort", onAbort);
        resolve();
      },
      (error) => {
        signal.removeEventListener("abort", onAbort);
        reject(error);
      },
    );
  });
}

async function activeCompactionRun(
  session: Session = new Session("sesn_1"),
  metrics?: RuntimeMetricsSink,
) {
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
      return Stream.fromAsyncIterable(
        (async function* () {
          if (options?.abortSignal === undefined) {
            throw new Error("compaction stream requires an abort signal");
          }
          await waitForReleaseOrAbort(providerRelease.promise, options.abortSignal);
          yield { type: "finish" as const, finishReason: "stop" as const };
        })(),
        (error): LLMServiceError => ({
          type: "llm-service",
          error: runtimeFailureFromProviderError(normalizeProviderError({
            code: "provider_stream_error",
            message: String(error),
            retryable: true,
          })),
        }),
      );
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
  const runFiber = Effect.runFork(
    Effect.gen(function* () {
      const agentLoop = yield* AgentLoop.Service;
      return yield* agentLoop.run(session, testRunCustody());
    }).pipe(
      Effect.provide(runtimeAgentLoopLayer(loader, {
        llmService: llm,
        writer,
        compaction: {},
        ...(metrics === undefined ? {} : { metrics }),
      })),
    ),
  );
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
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  throw new Error(`timed out waiting for ${label}`);
}

async function flushMicrotasks(count = 20): Promise<void> {
  for (let index = 0; index < count; index += 1) {
    await Promise.resolve();
  }
}

function failingEventWriter(
  appendedTypes: string[],
  shouldFail: (event: SessionEvent) => boolean,
): SessionEventWriter {
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

function runtimeAgentLoopLayer(
  loader: TestContextLoader,
  options: {
    readonly events?: readonly LLMEvent[];
    readonly store?: AgentLoopRuntimeStore;
    readonly writer?: SessionEventWriter;
    readonly llmService?: LLMServiceInterface;
    readonly onStream?: (request: LLMRequest) => void;
    readonly createProcessor?: Parameters<typeof AgentLoop.layer>[0]["createProcessor"];
    readonly providerCallRuntime?: ProviderCallRuntimeConfig;
    readonly providerCallAssembler?: ProviderCallAssembler;
    readonly compaction?: Parameters<typeof AgentLoop.layer>[0]["compaction"];
    readonly approvalMode?: Parameters<typeof AgentLoop.layer>[0]["approvalMode"];
    readonly runTool?: Parameters<typeof AgentLoop.layer>[0]["runTool"];
    readonly acceptSandboxExecution?: Parameters<typeof AgentLoop.layer>[0]["acceptSandboxExecution"];
    readonly awaitSandboxExecution?: Parameters<typeof AgentLoop.layer>[0]["awaitSandboxExecution"];
    readonly reviewApproval?: Parameters<typeof AgentLoop.layer>[0]["reviewApproval"];
    readonly runtimeModel?: Parameters<typeof AgentLoop.layer>[0]["runtimeModel"];
    readonly runtimePolicy?: Parameters<typeof AgentLoop.layer>[0]["runtimePolicy"];
    readonly runtime?: RuntimeDependencies;
    readonly metrics?: RuntimeMetricsSink;
    readonly refreshRuntimeBindingToken?: Parameters<typeof AgentLoop.layer>[0]["refreshRuntimeBindingToken"];
    readonly installLoaderState?: boolean;
  } = {},
): Layer.Layer<AgentLoop.Service> {
  const order: string[] = [];
  const store = options.store ?? new AgentLoopRuntimeStore(order);
  const writer = options.writer ?? writerFrom((envelope) => ({
    ok: true,
    writeId: envelope.writeId,
    eventId: `bridge-${envelope.writeId}`,
    processedAt: createdAt,
  }));
  const productionLayer = AgentLoop.layer({
    internalToolRepairStore: store,
    sessionEventWriter: writer,
    runtime: options.runtime ?? agentLoopRuntime(),
    llmService: options.llmService ?? llmService(options.events ?? [
      { type: "text-start", id: "text-1" },
      { type: "text-delta", id: "text-1", text_delta: "ok" },
      { type: "text-end", id: "text-1" },
      { type: "finish", finishReason: "stop" },
    ], options.onStream),
    storeOperationTimeoutMs: 1_000,
    ...(options.createProcessor !== undefined ? { createProcessor: options.createProcessor } : {}),
    providerCallRuntime: {
      ...DefaultProviderCallRuntimeConfig,
      timeoutMs: 1_800_000,
      ...options.providerCallRuntime,
      approvalReviewerPolicy: options.providerCallRuntime?.approvalReviewerPolicy ?? approvalReviewerPolicy,
    },
    ...(options.providerCallAssembler !== undefined ? { providerCallAssembler: options.providerCallAssembler } : {}),
    ...(options.compaction !== undefined ? { compaction: { timeoutMs: 1_800_000, ...options.compaction } } : {}),
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
  }).pipe(Layer.provide(AgentLoop.contextLoaderLayer(loader)));
  if (options.installLoaderState === false) {
    return productionLayer;
  }
  return Layer.effect(
    AgentLoop.Service,
    Effect.gen(function* () {
      const production = yield* AgentLoop.Service;
      return AgentLoop.Service.of({
        ...production,
        run: (session, custody) =>
          Effect.promise(() => installLoaderStateForTest(loader, session)).pipe(
            Effect.flatMap(() => production.run(session, custody)),
          ),
      });
    }).pipe(Effect.provide(productionLayer)),
  );
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

  recordHotState(_snapshot: RuntimeHotStateMetrics): void {}

  addActiveToolFibers(): void {}

  addPendingApprovals(): void {}

  observeProviderStreamDuration(kind: RuntimeProviderStreamKind, durationMs: number, outcome: RuntimeMetricOutcome): void {
    this.providerStreamDurations.push({ kind, durationMs, outcome });
  }

  observeEventWriteLatency(operation: RuntimeEventWriteOperation, durationMs: number, outcome: RuntimeMetricOutcome): void {
    this.eventWriteLatencies.push({ operation, durationMs, outcome });
  }

  observeContextLoadLatency(operation: RuntimeContextLoadOperation, durationMs: number, outcome: RuntimeMetricOutcome): void {
    this.contextLoadLatencies.push({ operation, durationMs, outcome });
  }

  recordCleanupCommandOutcome(): void {}
}

describe("AgentLoop", () => {
  test("pins effect to the approved beta version in package metadata", async () => {
    const packageJson = JSON.parse(await readFile(new URL("../../package.json", import.meta.url), "utf8")) as PackageJson;
    const lockfileText = await readFile(new URL("../../../../bun.lock", import.meta.url), "utf8");

    expect(packageJson.dependencies?.effect).toBe("4.0.0-beta.66");
    expect(lockfileText).toContain('"effect": "4.0.0-beta.66"');
    expect(lockfileText).toContain('"effect": ["effect@4.0.0-beta.66"');
    expect(lockfileText).not.toContain('"effect": "^4.0.0-beta.66"');
    expect(lockfileText).not.toContain('"effect": "4.0.0-beta.74"');
    expect(lockfileText).not.toContain('"effect": "3.');
  });

  test("default layer resolves a void session run frame", async () => {
    const loader = new RecordingContextLoader([], { type: "empty" });
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(new Session("sesn_1"), testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, { runtimeModel: () => undefined }))),
    );

    expect(result).toEqual({ type: "completed", modelMessageCount: 0 });
  });

  test("failed-run closeout writes a sanitized terminal error before idle settlement", async () => {
    const appended: SessionEvent[] = [];
    const writeIds: string[] = [];
    const loader = new RecordingContextLoader([], { type: "empty" });
    const session = new Session("sesn_failed_closeout");
    const defectCanary = "FAILED_RUN_DEFECT_SECRET_CANARY";

    await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        const closeout = agentLoop.closeFailedRun(
          session,
          new Error(defectCanary),
          testRunCustody("evt_failed_closeout_running"),
        );
        expect(yield* closeout).toEqual({ type: "landed" });
        expect(yield* closeout).toEqual({ type: "landed" });
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        writer: writerFrom((envelope) => {
          appended.push(envelope.event);
          writeIds.push(envelope.writeId);
          return {
            ok: true,
            writeId: envelope.writeId,
            eventId: `bridge-${envelope.writeId}`,
            processedAt: createdAt,
          };
        }),
        runtime: { ...agentLoopRuntime(), sleep: sleepUntilAborted },
      }))),
    );

    expect(appended.map((event) => event.type)).toEqual([
      "session.error",
      "session.status_idle",
    ]);
    expect(appended[0]).toMatchObject({
      type: "session.error",
      error: {
        code: "runtime_invalid_sequence",
        retryStatus: { type: "terminal" },
      },
    });
    expect(appended[1]).toEqual({ type: "session.status_idle", stop_reason: { type: "end_turn" } });
    expect(writeIds[0]).not.toBe(writeIds[1]);
    expect(JSON.stringify(appended)).not.toContain(defectCanary);
  });

  test("failed-run closeout observes one in-flight step across timeout windows and memoizes success", async () => {
    const errorResult = deferred<SessionEventWriterAppendResult>();
    let errorCalls = 0;
    let idleCalls = 0;
    let errorWriteId = "";
    const writer: SessionEventWriter = {
      ...writerFrom((envelope) => ({
        ok: true,
        writeId: envelope.writeId,
        eventId: `bridge-${envelope.writeId}`,
        processedAt: createdAt,
      })),
      append: async (envelope) => {
        errorCalls += 1;
        errorWriteId = envelope.writeId;
        return await errorResult.promise;
      },
      finishIdle: async (envelope) => {
        idleCalls += 1;
        return withFinishIdleReceiptForTest(envelope, {
          ok: true,
          writeId: envelope.durableTurnId,
          eventId: `bridge-${envelope.durableTurnId}`,
          processedAt: createdAt,
        });
      },
    };
    const loader = new RecordingContextLoader([], { type: "empty" });
    const session = new Session("sesn_failed_closeout_single_flight");
    let observationWindows = 0;
    const closeout = await Effect.runPromise(
      Effect.gen(function* () {
        return (yield* AgentLoop.Service).closeFailedRun(
          session,
          new Error("defect"),
          testRunCustody("evt_failed_closeout_single_flight_running"),
        );
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        writer,
        runtime: {
          ...agentLoopRuntime(),
          sleep: async (milliseconds, signal) => {
            if (
              milliseconds === SessionEventWriterRetryPolicy.timeoutPerAttemptMs
              && observationWindows < 2
            ) {
              observationWindows += 1;
              return true;
            }
            return await sleepUntilAborted(milliseconds, signal);
          },
        },
      }))),
    );

    expect(await Effect.runPromise(closeout)).toMatchObject({ type: "retry", error: { code: "timeout" } });
    expect(await Effect.runPromise(closeout)).toMatchObject({ type: "retry", error: { code: "timeout" } });
    expect(errorCalls).toBe(1);
    expect(idleCalls).toBe(0);

    errorResult.resolve({
      ok: true,
      writeId: errorWriteId,
      eventId: `bridge-${errorWriteId}`,
      processedAt: createdAt,
    });
    await flushMicrotasks(10);

    expect(await Effect.runPromise(closeout)).toEqual({ type: "landed" });
    expect(await Effect.runPromise(closeout)).toEqual({ type: "landed" });
    expect(errorCalls).toBe(1);
    expect(idleCalls).toBe(1);
  });

  test("failed-run closeout shares one observation window across error and idle steps", async () => {
    const errorResult = deferred<SessionEventWriterAppendResult>();
    const idleResult = deferred<SessionEventWriterAppendResult>();
    const observationElapsed = deferred<void>();
    let errorWriteId = "";
    let idleWriteId = "";
    let errorCalls = 0;
    let idleCalls = 0;
    let timeoutSleeps = 0;
    const writer: SessionEventWriter = {
      ...writerFrom((envelope) => ({
        ok: true,
        writeId: envelope.writeId,
        eventId: `bridge-${envelope.writeId}`,
        processedAt: createdAt,
      })),
      append: async (envelope) => {
        errorCalls += 1;
        errorWriteId = envelope.writeId;
        return await errorResult.promise;
      },
      finishIdle: async (envelope) => {
        idleCalls += 1;
        idleWriteId = envelope.durableTurnId;
        return withFinishIdleReceiptForTest(envelope, await idleResult.promise);
      },
    };
    const closeout = await Effect.runPromise(
      Effect.gen(function* () {
        return (yield* AgentLoop.Service).closeFailedRun(
          new Session("sesn_failed_closeout_shared_window"),
          new Error("defect"),
          testRunCustody("evt_failed_closeout_shared_window_running"),
        );
      }).pipe(Effect.provide(runtimeAgentLoopLayer(
        new RecordingContextLoader([], { type: "empty" }),
        {
          writer,
          runtime: {
            ...agentLoopRuntime(),
            sleep: async (milliseconds, signal) => {
              if (milliseconds === SessionEventWriterRetryPolicy.timeoutPerAttemptMs) {
                timeoutSleeps += 1;
                if (timeoutSleeps > 1) {
                  return await sleepUntilAborted(milliseconds, signal);
                }
                await Promise.race([
                  observationElapsed.promise,
                  new Promise<void>((resolve) => signal.addEventListener("abort", () => resolve(), { once: true })),
                ]);
                return true;
              }
              return await sleepUntilAborted(milliseconds, signal);
            },
          },
        },
      ))),
    );

    const first = Effect.runPromise(closeout);
    await waitForCondition(() => errorCalls === 1, "failed-run error closeout start");
    errorResult.resolve({
      ok: true,
      writeId: errorWriteId,
      eventId: `bridge-${errorWriteId}`,
      processedAt: createdAt,
    });
    await waitForCondition(() => idleCalls === 1, "failed-run idle closeout start");
    observationElapsed.resolve();
    expect(await first).toMatchObject({ type: "retry", error: { code: "timeout" } });
    expect(timeoutSleeps).toBe(2);

    idleResult.resolve({
      ok: true,
      writeId: idleWriteId,
      eventId: `bridge-${idleWriteId}`,
      processedAt: createdAt,
    });
    await flushMicrotasks(10);
    expect(await Effect.runPromise(closeout)).toEqual({ type: "landed" });
    expect(errorCalls).toBe(1);
    expect(idleCalls).toBe(1);
  });

  test("failed-run closeout retries a rejected step with the same write identity on the next cycle", async () => {
    const appendWriteIds: string[] = [];
    let appendCalls = 0;
    const writer: SessionEventWriter = {
      ...writerFrom((envelope) => ({
        ok: true,
        writeId: envelope.writeId,
        eventId: `bridge-${envelope.writeId}`,
        processedAt: createdAt,
      })),
      append: async (envelope) => {
        appendCalls += 1;
        appendWriteIds.push(envelope.writeId);
        if (appendCalls <= SessionEventWriterRetryPolicy.attempts) {
          return {
            ok: false,
            error: normalizeSessionEventWriterError({
              code: "unavailable",
              sessionId: envelope.sessionId,
              writeId: envelope.writeId,
            }),
          };
        }
        return {
          ok: true,
          writeId: envelope.writeId,
          eventId: `bridge-${envelope.writeId}`,
          processedAt: createdAt,
        };
      },
    };
    const closeout = await Effect.runPromise(
      Effect.gen(function* () {
        return (yield* AgentLoop.Service).closeFailedRun(
          new Session("sesn_failed_closeout_reissue"),
          new Error("defect"),
          testRunCustody("evt_failed_closeout_reissue_running"),
        );
      }).pipe(Effect.provide(runtimeAgentLoopLayer(
        new RecordingContextLoader([], { type: "empty" }),
        {
          writer,
          runtime: {
            ...agentLoopRuntime(),
            sleep: async (milliseconds, signal) =>
              milliseconds === SessionEventWriterRetryPolicy.timeoutPerAttemptMs
                ? await sleepUntilAborted(milliseconds, signal)
                : true,
          },
        },
      ))),
    );

    expect(await Effect.runPromise(closeout)).toMatchObject({
      type: "retry",
      error: { code: "unavailable" },
    });
    expect(await Effect.runPromise(closeout)).toEqual({ type: "landed" });
    expect(appendWriteIds).toHaveLength(SessionEventWriterRetryPolicy.attempts + 1);
    expect(new Set(appendWriteIds).size).toBe(1);
  });

  test("failed-run closeout classifies an acknowledgement mismatch as unrepairable", async () => {
    const loader = new RecordingContextLoader([], { type: "empty" });
    const session = new Session("sesn_failed_closeout_ack_mismatch");

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.closeFailedRun(
          session,
          new Error("defect"),
          testRunCustody("evt_failed_closeout_ack_mismatch_running"),
        );
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        writer: writerFrom((envelope) => ({
          ok: true,
          writeId: `${envelope.writeId}_divergent`,
          eventId: `bridge-${envelope.writeId}`,
          processedAt: createdAt,
        })),
        runtime: { ...agentLoopRuntime(), sleep: sleepUntilAborted },
      }))),
    );

    expect(result).toMatchObject({
      type: "unrepairable",
      error: { code: "ack_mismatch" },
    });
  });

  test("reports declaration, event-write, and provider stream metrics through injected sink", async () => {
    const metrics = new RecordingRuntimeMetrics();
    const loader = new RecordingContextLoader([], {
      type: "messages",
      messages: [userMessage("msg_user_1", 1, "hello")],
    });

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(new Session("sesn_1"), testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, { metrics }))),
    );

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
      const session = new Session(`sesn_agent_system_${scenario.name.replaceAll(" ", "_")}`);
      const requests: LLMRequest[] = [];
      const loader = new RecordingContextLoader([], {
        type: "messages",
        messages: [userMessage(`user-agent-system-${scenario.name}`, 0, "hello")],
      });
      const result = await Effect.runPromise(
        Effect.gen(function* () {
          const agentLoop = yield* AgentLoop.Service;
          return yield* agentLoop.run(session, testRunCustody());
        }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
          onStream: (request) => requests.push(request),
          runtimePolicy: () => runtimeToolPolicyFromPatchPayloads(scenario.patches),
        }))),
      );

      expect(result).toMatchObject({ type: "completed" });
      expect(requests).toHaveLength(1);
      expect(requests[0]?.system.filter((segment) =>
        segment.kind === SystemSegmentKind.SYSTEM_SEGMENT_KIND_AGENT
      )).toEqual([...scenario.expectedAgentSegments]);
      expect(requests[0]?.system.filter((segment) =>
        segment.kind === SystemSegmentKind.SYSTEM_SEGMENT_KIND_MEMORY
      )).toEqual([...scenario.expectedMemorySegments]);
    }
  });

  test("provider snapshot injects apply-patch instructions from the cold pinned GPT family", async () => {
    const session = new Session("sesn_gpt_patch_prompt");
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

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        onStream: (request) => requests.push(request),
        runtimePolicy: () => runtimeToolPolicyFromPatchPayloads([payloadJson]),
      }))),
    );

    expect(result).toMatchObject({ type: "completed" });
    expect(requests[0]?.system[0]?.text).toContain("## `apply_patch`");
    expect(requests[0]?.system[0]?.text).toContain("do not JSON-wrap it");
  });

  test("refreshes the binding token without advancing the request anchor past its committed snapshot", async () => {
    const session = new Session("sesn_1");
    const requests: LLMRequest[] = [];
    const identities: string[] = [];
    const loader = new RecordingContextLoader(
      [],
      { type: "messages", messages: [userMessage("user-refresh", 0, "refresh token")] },
    );

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        events: [
          { type: "text-start", id: "text-1" },
          { type: "text-delta", id: "text-1", text_delta: "ok" },
          { type: "text-end", id: "text-1" },
          {
            type: "finish",
            finishReason: "stop",
            usage: { inputTokens: 10, outputTokens: 2, reasoningTokens: 0, cacheReadTokens: 0, cacheWriteTokens: 0 },
            modelLimits: { contextWindowTokens: 320, outputTokenLimit: 120 },
          },
        ],
        onStream: (request) => requests.push(request),
        refreshRuntimeBindingToken: async (identity) => {
          identities.push(identity.runtimeBindingToken);
          session.state.contextManager.appendMessage({
            ...runtimeNotificationMessage("msg_task_during_refresh", "committed after request snapshot"),
            sequence: 1,
          });
          session.state.addTransientModelMessage({
            ...runtimeNotificationMessage("msg_note_during_refresh", "transient after request snapshot"),
            sequence: 2,
          });
          return "runtime-binding-token-refreshed";
        },
      }))),
    );

    expect(result).toMatchObject({ type: "completed" });
    expect(identities).toEqual(["runtime-binding-token-test"]);
    expect(requests[0]?.runtimeBindingToken).toBe("runtime-binding-token-refreshed");
    expect(JSON.stringify(requests[0]?.messages)).not.toContain("committed after request snapshot");
    expect(JSON.stringify(requests[0]?.messages)).not.toContain("transient after request snapshot");
    expect(session.identity.runtimeBindingToken).toBe("runtime-binding-token-refreshed");
    expect(session.state.lastRequestContextAnchorSequence()).toBe(
      session.state.contextManager.messages().find((message) => message.role === "user")?.sequence,
    );
    expect(JSON.stringify(session.state.transientModelMessages())).toContain("transient after request snapshot");
  });

  test("loads cold context and pending messages without deriving the configured model from either message list", async () => {
    const history = [userMessage("user-1", 0, "first")];
    const pending = {
      type: "messages",
      messages: [
        userMessage("user-2", 1, "second"),
        userMessage("user-3", 2, "third"),
      ],
    } as const satisfies PendingInputResult;
    const loader = new RecordingContextLoader(history, pending);
    const session = new Session("sesn_1");

    // The supplier returns a model no message in either fixture list carries,
    // so a reintroduced derivation from ANY message (first-wins or last-wins)
    // produces a mismatch here instead of passing by coincidence.
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
            runtimeModel: () => ({ providerId: "resolved", modelId: "from-config" }),
          }),
        ),
      ),
    );

    expect(result).toEqual({
      type: "completed",
      currentModel: { providerId: "resolved", modelId: "from-config" },
      modelMessageCount: 3,
    });
    expect(loader.buildCalls).toEqual(["sesn_1"]);
    expect(loader.pendingCalls).toEqual(["sesn_1"]);
    expect(session.state.contextManager.messages().map((message) => message.role)).toEqual(["user", "user", "user", "assistant"]);
    expect(JSON.stringify(session.state.contextManager.messages())).not.toContain("system prompt");
    expect(JSON.stringify(session.state.contextManager.messages())).not.toContain("toolDefinitions");
  });

  test("assembles non-persistent runtime inputs into LLMRequest without storing them in hot or durable messages", async () => {
    const session = new Session("sesn_1");
    const writtenPayloads: unknown[] = [];
    const store = new AgentLoopRuntimeStore([], false, false, (_operation, payload) => {
      writtenPayloads.push(payload);
    });
    const capturedRequests: LLMRequest[] = [];
    const systemCanary = "third group system instruction canary";
    const toolDescriptionCanary = "third group tool description canary";
    const toolSchemaCanary = "third group schema canary";
    const providerConfigCanary = "third group provider config canary";
    const dummyTokenCanary = "dummy-thirdgroup-token";
    const tools = [
      {
        name: "third_group_lookup",
        description: toolDescriptionCanary,
        inputSchema: {
          type: "object",
          properties: {
            query: { type: "string", description: toolSchemaCanary },
            providerConfigMarker: { const: providerConfigCanary },
            dummyTokenMarker: { const: dummyTokenCanary },
          },
        },
      },
    ];
    const runtimeBoundary: ProviderCallRuntimeConfig = {
      systemInstructions: systemCanary,
      toolCatalog: catalogForTest(tools[0]!),
      maxOutputTokens: 321,
      timeoutMs: 777,
    };
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
            store,
            providerCallRuntime: runtimeBoundary,
            onStream: (request) => {
              capturedRequests.push(request);
            },
          }),
        ),
      ),
    );

    expect(result).toMatchObject({ type: "completed", modelMessageCount: 1 });
    expect(capturedRequests).toHaveLength(1);
    expect(capturedRequests[0]).toMatchObject({
      requestKind: ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
      workspaceId: "workspace-test",
      sessionId: "sesn_1",
      sessionThreadId: "thread-test",
      bindingId: "binding-test",
      bindingGeneration: 1,
      runtimeBindingToken: "runtime-binding-token-test",
      model: { providerId: "fake", modelId: "fake-chat", variant: "" },
      system: [
        {
          kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE,
          text: systemCanary,
          cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_STABLE,
        },
      ],
      messages: [
        {
          role: RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_USER,
          status: "completed",
          origin: "user",
          parts: [{ text: { text: "hello" } }],
        },
      ],
      tools: [
        {
          name: "third_group_lookup",
          description: toolDescriptionCanary,
          inputSchemaJson: JSON.stringify(tools[0]?.inputSchema),
        },
      ],
      attachments: [],
      limits: { maxOutputTokens: 321, timeoutMs: 777 },
    });
    const hotContext = JSON.stringify(session.state.contextManager.messages());
    const durableWrites = JSON.stringify(writtenPayloads);
    for (const canary of [systemCanary, toolDescriptionCanary, toolSchemaCanary, providerConfigCanary, dummyTokenCanary, "maxOutputTokens", "timeoutMs"]) {
      expect(hotContext).not.toContain(canary);
      expect(durableWrites).not.toContain(canary);
    }
  });

  test("empty cold history still processes pending input", async () => {
    const pendingLoader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });

    const pendingResult = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(new Session("sesn_1"), testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(pendingLoader))),
    );

    expect(pendingResult).toMatchObject({ type: "completed", modelMessageCount: 1 });
  });

  test("first accepted turn resolves its provider model from the cold runtime config", async () => {
    const session = new Session("sesn_first_config_model");
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

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        return yield* (yield* AgentLoop.Service).run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        runtimeModel: (activeSession) => runtimeModelForThread(
          activeSession.identity.threadRole,
          activeSession.configuration.patches().map((patch) => patch.payloadJson),
          { providerId: "anthropic", modelId: "claude-opus-4-8" },
        ),
        onStream: (request) => requests.push(request),
      }))),
    );

    expect(result).toMatchObject({ type: "completed" });
    expect(requests).toHaveLength(1);
    expect(requests[0]?.model).toEqual({ providerId: "openai", modelId: "gpt-5.5", variant: "" });
  });

  test("lost CommitInputs response retries the frozen declaration without duplicating hot input", async () => {
    const session = new Session("sesn_lost_commit_response");
    const input = acceptedInput("rin_lost_commit_response", session.sessionId);
    session.state.enqueueAcceptedInput(input);
    const submittedDrafts: string[] = [];
    let attempts = 0;
    const loader: TestContextLoader = {
      buildContext: async () => [],
      loadPendingInput: async () => ({ type: "empty" }),
      commitAcceptedInput: async (accepted, options) => {
        attempts += 1;
        submittedDrafts.push(JSON.stringify(options?.drafts ?? []));
        if (attempts === 1) {
          throw normalizeContextLoaderError({
            code: "unavailable",
            sessionId: accepted.sessionId,
            reason: "commit response was lost",
          });
        }
        return acceptedInputReceipt(accepted, "duplicate");
      },
    };

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        return yield* (yield* AgentLoop.Service).run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader))),
    );

    expect(result).toMatchObject({ type: "completed" });
    expect(attempts).toBe(2);
    expect(new Set(submittedDrafts).size).toBe(1);
    expect(session.state.contextManager.messages().filter((message) => message.origin === "user")).toHaveLength(1);
  });

  test("a stale CommitInputs receipt discards hot state without terminal declaration writes", async () => {
    const session = new Session("sesn_stale_commit_receipt");
    const input = acceptedInput("rin_stale_commit_receipt", session.sessionId);
    session.state.enqueueAcceptedInput(input);
    const terminalEvents: string[] = [];
    const committed = acceptedInputReceipt(input);
    const loader: TestContextLoader = {
      buildContext: async () => [],
      loadPendingInput: async () => ({ type: "empty" }),
      commitAcceptedInput: async () => ({
        ...committed,
        applicationDisposition: "stale_custody",
      }),
    };

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        return yield* (yield* AgentLoop.Service).run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        writer: writerFrom((envelope) => {
          if (envelope.event.type === "session.error" || envelope.event.type === "span.model_request_end") {
            terminalEvents.push(envelope.event.type);
          }
          return {
            ok: true,
            writeId: envelope.writeId,
            eventId: `bridge-${envelope.writeId}`,
            processedAt: createdAt,
          };
        }),
      }))),
    );

    expect(result).toEqual({ type: "interrupted", discardHotState: true });
    expect(session.state.contextManager.messages()).toEqual([]);
    expect(session.state.acceptedInputCount()).toBe(0);
    expect(terminalEvents).toEqual([]);
  });

  test("a bounded live rejection is authored by the loop and committed before provider work", async () => {
    const session = new Session("sesn_live_rejection");
    const input = {
      ...acceptedInput("rin_live_rejection", session.sessionId),
      kind: "rejection" as const,
      reasonCode: "runtime_command_payload_too_large" as const,
    };
    session.state.enqueueAcceptedInput(input);
    const submittedDrafts: Array<readonly unknown[]> = [];
    const loader: TestContextLoader = {
      buildContext: async () => [],
      loadPendingInput: async () => ({ type: "empty" }),
      commitAcceptedInput: async (accepted, options) => {
        submittedDrafts.push([...(options?.drafts ?? [])]);
        return acceptedInputReceipt(accepted);
      },
    };

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        return yield* (yield* AgentLoop.Service).run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader))),
    );

    expect(result).toMatchObject({ type: "completed" });
    expect(submittedDrafts).toEqual([[
      expect.objectContaining({
        role: "assistant",
        origin: "agent",
        status: "completed",
        parts: [expect.objectContaining({
          type: "text",
          text: "The session runtime could not accept this input.",
        })],
      }),
    ]]);
    expect(session.state.contextManager.messages()).toContainEqual(
      expect.objectContaining({
        owningEventId: "sevt_rin_live_rejection",
        role: "assistant",
        parts: [expect.objectContaining({
          type: "text",
          text: "The session runtime could not accept this input.",
        })],
      }),
    );
  });

  test("first accepted turn rides the file attachments returned by CommitInputs", async () => {
    const session = new Session("sesn_first_turn_media");
    const input = acceptedInput("rin_first_turn_media", session.sessionId);
    session.state.enqueueAcceptedInput(input);
    const attachment = {
      transient: undefined,
      fileBacked: {
        sourceEventId: input.eventIds[0]!,
        fileId: "file_first_turn_media",
      },
      mime: "image/png",
      filename: "first-turn.png",
    } as const;
    const loader: TestContextLoader = {
      buildContext: async () => [],
      loadPendingInput: async () => ({ type: "empty" }),
      commitAcceptedInput: async (accepted) => {
        const committed = acceptedInputReceipt(accepted);
        return {
          ...committed,
          receipt: {
            ...committed.receipt,
            pendingAttachmentDelta: [attachment],
          },
        };
      },
    };
    const requests: LLMRequest[] = [];

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        return yield* (yield* AgentLoop.Service).run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        onStream: (request) => requests.push(request),
      }))),
    );

    expect(result).toMatchObject({ type: "completed" });
    expect(requests).toHaveLength(1);
    expect(requests[0]?.attachments).toEqual([attachment]);
  });

  test("accepted input without a resolvable runtime model settles an explicit exhausted error", async () => {
    const session = new Session("sesn_missing_config_model");
    session.state.enqueueAcceptedInput(acceptedInput("rin_missing_config_model", session.sessionId));
    const loader = new QueuedContextLoader([], []);
    const appended: SessionEvent[] = [];

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        return yield* (yield* AgentLoop.Service).run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        runtimeModel: () => undefined,
        writer: writerFrom((envelope) => {
          appended.push(envelope.event);
          return {
            ok: true,
            writeId: envelope.writeId,
            eventId: `bridge-${envelope.writeId}`,
            processedAt: createdAt,
          };
        }),
      }))),
    );

    expect(result).toMatchObject({
      type: "failed",
      error: {
        code: "runtime_invalid_sequence",
        fatal: true,
        reason: "runtime_contract_validation",
      },
    });
    expect(appended).toEqual([
      { type: "session.status_running" },
      expect.objectContaining({
        type: "session.error",
        error: expect.objectContaining({
          code: "runtime_invalid_sequence",
          retryStatus: { type: "exhausted" },
        }),
      }),
      { type: "session.status_idle", stop_reason: { type: "retries_exhausted" } },
    ]);
  });

  test("no accepted pending input performs no durable turn transition", async () => {
    const loader = new RecordingContextLoader([], { type: "empty" });
    const session = new Session("sesn_1");
    const appendedTypes: string[] = [];

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
            runtimeModel: () => undefined,
            writer: writerFrom((envelope) => {
              appendedTypes.push(envelope.event.type);
              return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
            }),
          }),
        ),
      ),
    );

    expect(result).toEqual({ type: "completed", modelMessageCount: 0 });
    expect(appendedTypes).toEqual([]);
  });

  test("idle finalization fails closed when FinishIdle boundary is unavailable", async () => {
    const loader = new RecordingContextLoader([], { type: "empty" });
    const appendedTypes: string[] = [];
    const writer: SessionEventWriter = {
      append: async (envelope) => {
        appendedTypes.push(envelope.event.type);
        return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
      },
      writeRequestEnd: async () => {
        throw new Error("idle-only test must not close a provider request");
      },
    };

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(new Session("sesn_1"), testRunCustody("evt_open_idle_failure"));
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, { writer }))),
    );

    expect(result).toMatchObject({
      type: "failed",
      error: { code: "unavailable", sessionId: "sesn_1" },
      releaseSession: { reason: "event_write_failed" },
    });
    expect(appendedTypes).toEqual([]);
  });

  test("idle finalization retries lost ACKs with the same runtime write id", async () => {
    const loader = new RecordingContextLoader([], { type: "empty" });
    const finishIdleWriteIds: string[] = [];
    const writer: SessionEventWriter = {
      append: async (envelope) => ({
        ok: true,
        writeId: envelope.writeId,
        eventId: `bridge-${envelope.writeId}`,
        processedAt: createdAt,
      }),
      writeRequestEnd: async () => {
        throw new Error("idle-only test must not close a provider request");
      },
      finishIdle: async (envelope) => {
        finishIdleWriteIds.push(envelope.durableTurnId);
        if (finishIdleWriteIds.length === 1) {
          return {
            ok: false,
            error: {
              type: "session-event-writer",
              code: "timeout",
              message: "lost ack",
              retryable: true,
              fatal: false,
              sessionId: envelope.sessionId,
              writeId: envelope.durableTurnId,
            },
          };
        }
        return withFinishIdleReceiptForTest(envelope, {
          ok: true,
          writeId: envelope.durableTurnId,
          eventId: `bridge-${envelope.durableTurnId}`,
          processedAt: createdAt,
        });
      },
    };

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(new Session("sesn_1"), testRunCustody("evt_open_idle_retry"));
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, { runtimeModel: () => undefined, writer }))),
    );

    expect(result).toEqual({ type: "completed", modelMessageCount: 0 });
    expect(finishIdleWriteIds).toEqual(["evt_open_idle_retry", "evt_open_idle_retry"]);
  });

  test("idle finalization drains the raw FinishIdle call after its local timeout", async () => {
    const rawFinish = deferred<SessionEventWriterAppendResult>();
    let finishCalls = 0;
    const writer: SessionEventWriter = {
      append: async (envelope) => ({
        ok: true,
        writeId: envelope.writeId,
        eventId: `bridge-${envelope.writeId}`,
        processedAt: createdAt,
      }),
      writeRequestEnd: async () => {
        throw new Error("idle-only test must not close a provider request");
      },
      finishIdle: async (envelope) => {
        finishCalls += 1;
        return withFinishIdleReceiptForTest(envelope, await rawFinish.promise);
      },
    };
    const run = Effect.runPromise(
      Effect.gen(function* () {
        return yield* (yield* AgentLoop.Service).run(
          new Session("sesn_finish_idle_drain"),
          testRunCustody("evt_open_idle_drain"),
        );
      }).pipe(Effect.provide(runtimeAgentLoopLayer(
        new RecordingContextLoader([], { type: "empty" }),
        { runtimeModel: () => undefined, writer, runtime: { ...agentLoopRuntime(), sleep: async () => true } },
      ))),
    );
    let settled = false;
    void run.finally(() => {
      settled = true;
    });
    await flushMicrotasks(10);
    expect(finishCalls).toBe(1);
    expect(settled).toBe(false);

    rawFinish.resolve({
      ok: true,
      writeId: "evt_open_idle_drain",
      eventId: "bridge-evt_open_idle_drain",
      processedAt: createdAt,
    });
    expect(await run).toEqual({ type: "completed", modelMessageCount: 0 });
    expect(finishCalls).toBe(1);
  });

  test("a second warm turn preserves model and ContextManager state without context reads", async () => {
    const loader = new QueuedContextLoader([], []);
    const session = new Session("sesn_1");
    const layer = runtimeAgentLoopLayer(loader, { installLoaderState: false });

    session.state.enqueueAcceptedInput(acceptedInput("rin_warm_first"));
    const first = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(layer)),
    );
    session.state.enqueueAcceptedInput(acceptedInput("rin_warm_second"));
    const second = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(layer)),
    );
    const third = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(layer)),
    );

    expect(first).toMatchObject({ type: "completed", currentModel: { providerId: "fake", modelId: "fake-chat" } });
    expect(second).toMatchObject({ type: "completed", currentModel: { providerId: "fake", modelId: "fake-chat" } });
    expect(third).toMatchObject({ type: "completed", currentModel: { providerId: "fake", modelId: "fake-chat" } });
    expect(loader.buildCalls).toEqual([]);
    expect(loader.pendingCalls).toEqual([]);
    expect(session.state.contextManager.messages().map((message) => message.role)).toEqual(["user", "assistant", "user", "assistant"]);
  });

  test("projection failure returns a failed run instead of masking invalid context as completed", async () => {
    const loader = new RecordingContextLoader([], { type: "empty" });
    const session = new Session("sesn_1");
    session.state.contextManager.appendMessage({
      id: "system-1",
      sessionId: "sesn_1",
      role: "system",
      sequence: 0,
      status: "completed",
      createdAt,
      parts: [
        {
          id: "system-1-text",
          sessionId: "sesn_1",
          messageId: "system-1",
          sequence: 0,
          type: "text",
          text: "system prompt",
          truncated: false,
          status: "completed",
          createdAt,
        },
      ],
    } as unknown as RuntimeMessage);
    session.state.markPersistentContextLoaded();
    const appendedTypes: string[] = [];

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
            writer: writerFrom((envelope) => {
              appendedTypes.push(envelope.event.type);
              return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
            }),
          }),
        ),
      ),
    );

    expect(result).toMatchObject({
      type: "failed",
      releaseSession: { reason: "crashed" },
      error: {
        code: "provider_invalid_request",
        message: "Runtime context is not valid for Gateway ProviderRequest: schema.",
      },
    });
    expect(appendedTypes).toEqual([]);
    expect(session.state.contextManager.messages()).toEqual([]);
  });

  test("runtime layer gates assistant progress hot context on durable event ACKs", async () => {
    const order: string[] = [];
    const session = new Session("sesn_1");
    const store = new AgentLoopRuntimeStore(order);
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    let providerSawShell = false;
    const writer = writerFrom((envelope) => {
      const assistant = session.state.contextManager.messages().find((message) => message.role === "assistant");
      order.push(`event:${envelope.event.type}:context_parts_${assistant?.parts.length ?? 0}`);
      return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
    });
    await installLoaderStateForTest(loader, session);

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          AgentLoop.runtimeLayer({
            internalToolRepairStore: store,
            sessionEventWriter: writer,
            runtime: agentLoopRuntime(),
            llmService: llmService([
              { type: "text-start", id: "text-1" },
              { type: "text-delta", id: "text-1", text_delta: "hello" },
              { type: "text-end", id: "text-1" },
              { type: "finish", finishReason: "tool-calls" },
            ], () => {
              const assistant = session.state.contextManager.messages().find((message) => message.role === "assistant");
              providerSawShell = assistant?.status === "streaming" && assistant.parts.length === 0;
              order.push("provider:stream");
            }),
            storeOperationTimeoutMs: 1_000,
            providerCallRuntime: { ...DefaultProviderCallRuntimeConfig, timeoutMs: 1_800_000 },
            runtimeModel: () => ({ providerId: "fake", modelId: "fake-chat" }),
          }).pipe(Layer.provide(AgentLoop.contextLoaderLayer(loader))),
        ),
      ),
    );

    expect(result).toMatchObject({ type: "completed", modelMessageCount: 1 });
    expect(providerSawShell).toBe(false);
    expect(order).toEqual([
      "event:session.status_running:context_parts_0",
      "event:span.model_request_start:context_parts_0",
      "provider:stream",
      "event:agent.message:context_parts_0",
      "event:span.model_request_end:context_parts_1",
      "event:session.status_idle:context_parts_1",
    ]);
    expect(session.state.contextManager.messages().map((message) => message.role)).toEqual(["user", "assistant"]);
    expect(session.state.contextManager.messages().at(-1)?.parts).toEqual([
      expect.objectContaining({ type: "text", text: "hello", status: "completed" }),
    ]);
  });

  test("runtime layer emits running, span, progress, span end, and idle around a normal provider call", async () => {
    const session = new Session("sesn_1");
    const store = new AgentLoopRuntimeStore([]);
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const timeline: string[] = [];
    const appended: SessionEvent[] = [];
    const writer = writerFrom((envelope) => {
      timeline.push(`event:${envelope.event.type}`);
      appended.push(envelope.event);
      return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
    });

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
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
                  contextWindowTokens: 400_000,
                  inputLimitTokens: 272_000,
                  outputTokenLimit: 128_000,
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
      contextWindowTokens: 400_000,
      inputLimitTokens: 272_000,
      outputTokenLimit: 128_000,
    });
  });

  test("a provider request does not start when WriteRequestEnd transport is unavailable", async () => {
    const session = new Session("sesn_1");
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

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(runtimeAgentLoopLayer(loader, {
          writer: malformedWriter,
          onStream: (request) => providerRequests.push(request),
          events: [
            { type: "text-start", id: "text-1" },
            { type: "text-delta", id: "text-1", text_delta: "hello" },
            { type: "text-end", id: "text-1" },
            { type: "finish", finishReason: "stop" },
          ],
        })),
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

  test("approval reviewer sessions mark provider requests, request-end events, and metrics as reviewer work", async () => {
    const session = new Session({
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
      writeId: envelope.writeId,
      eventId: `bridge-${envelope.writeId}`,
      processedAt: createdAt,
    }));
    const writer: SessionEventWriter = {
      ...baseWriter,
      writeRequestEnd: async (envelope) => {
        requestEndEnvelopes.push(envelope);
        return await baseWriter.writeRequestEnd(envelope);
      },
    };
    const metrics = new RecordingRuntimeMetrics();
    const llm = queuedLLMService([[
      { type: "text-start", id: "text-1" },
      { type: "text-delta", id: "text-1", text_delta: "allow" },
      { type: "text-end", id: "text-1" },
      {
        type: "finish",
        finishReason: "stop",
        usage: { inputTokens: 3, outputTokens: 1, reasoningTokens: 0, cacheReadTokens: 0, cacheWriteTokens: 0 },
        modelLimits: { contextWindowTokens: 100_000, outputTokenLimit: 4_096 },
      },
    ]], requests);

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        llmService: llm,
        metrics,
        writer,
        runtimeModel: () => ({ providerId: "anthropic", modelId: "claude-opus-4-8" }),
      }))),
    );

    expect(result).toMatchObject({ type: "completed", modelMessageCount: 1 });
    expect(requests).toHaveLength(1);
    expect(requests[0]?.requestKind).toBe(ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER);
    expect(requests[0]?.outputSchemaJson).toBe(approvalReviewerOutputSchemaJson);
    expect(requests[0]?.system).toContainEqual({
      kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_APPROVAL_REVIEWER_POLICY,
      text: approvalReviewerPolicy,
      cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_STABLE,
    });
    expect(requests[0]?.model).toEqual({ providerId: "anthropic", modelId: "claude-opus-4-8", variant: "" });
    expect(requestEndEnvelopes.map((envelope) => envelope.requestKind)).toEqual(["approval_reviewer"]);
    expect(metrics.providerStreamDurations).toContainEqual(expect.objectContaining({
      kind: "approval_reviewer",
      outcome: "success",
    }));
    expect(session.state.lastRequestUsage()).toEqual({
      inputTokens: 3,
      outputTokens: 1,
      reasoningTokens: 0,
      cacheReadTokens: 0,
      cacheWriteTokens: 0,
    });
    expect(session.state.lastRequestModelLimits()).toEqual({
      contextWindowTokens: 100_000,
      outputTokenLimit: 4_096,
    });
  });

  test("accepted declarations preserve approval-reviewer thread metadata without reloading context", async () => {
    const session = new Session({
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
    session.state.recordLastRequestCompletion({
      inputTokens: 50,
      outputTokens: 5,
      reasoningTokens: 0,
      cacheReadTokens: 0,
      cacheWriteTokens: 0,
    }, {
      contextWindowTokens: 100_000,
      outputTokenLimit: 4_096,
    }, 0);
    session.state.enqueueAcceptedInput(approvalReviewAcceptedInput("rin_reviewer_context"));
    const loader = new QueuedContextLoader([], []);
    const requests: LLMRequest[] = [];
    const requestEndEnvelopes: SessionEventWriterRequestEndEnvelope[] = [];
    const baseWriter = writerFrom((envelope) => ({
      ok: true,
      writeId: envelope.writeId,
      eventId: `bridge-${envelope.writeId}`,
      processedAt: createdAt,
    }));
    const writer: SessionEventWriter = {
      ...baseWriter,
      writeRequestEnd: async (envelope) => {
        requestEndEnvelopes.push(envelope);
        return await baseWriter.writeRequestEnd(envelope);
      },
    };
    const llm = queuedLLMService([[
      { type: "text-start", id: "text-1" },
      { type: "text-delta", id: "text-1", text_delta: "allow" },
      { type: "text-end", id: "text-1" },
      { type: "finish", finishReason: "stop" },
    ]], requests);

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, { llmService: llm, writer }))),
    );

    expect(result).toMatchObject({ type: "completed", modelMessageCount: 1 });
    expect(session.identity).toMatchObject({
      parentThreadId: "thrd_main",
      threadRole: "approval_reviewer",
      runtimeBindingToken: "binding-token-before-commit",
    });
    expect(requests[0]?.requestKind).toBe(ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER);
    expect(requestEndEnvelopes.map((envelope) => envelope.requestKind)).toEqual(["approval_reviewer"]);
    expect(session.state.lastRequestUsage()).toBeUndefined();
    expect(session.state.lastRequestModelLimits()).toBeUndefined();
  });

  test("a reviewer finish arms proactive compaction on the reviewer model before its next review", async () => {
    const session = new Session({
      workspaceId: "wksp_reviewer",
      sessionId: "sesn_reviewer_proactive_compaction",
      sessionThreadId: "thrd_reviewer",
      parentThreadId: "thrd_main",
      threadRole: "approval_reviewer",
      bindingId: "bind_reviewer",
      bindingGeneration: 1,
      targetPodUid: "pod_reviewer",
      runtimeBindingToken: "binding-token-reviewer",
    });
    session.state.enqueueAcceptedInput({
      ...approvalReviewAcceptedInput("rin_reviewer_first"),
      promptItems: [userMessage("user-review-first", 0, compactionHistory("review the first action"))],
    });
    const loader = new QueuedContextLoader([], []);
    const requests: LLMRequest[] = [];
    const llm = queuedLLMService([
      [
        { type: "text-start", id: "review-first" },
        { type: "text-delta", id: "review-first", text_delta: "allow" },
        { type: "text-end", id: "review-first" },
        {
          type: "finish",
          finishReason: "stop",
          usage: { inputTokens: 96_000, outputTokens: 1, reasoningTokens: 0, cacheReadTokens: 0, cacheWriteTokens: 0 },
          modelLimits: { contextWindowTokens: 100_000, outputTokenLimit: 4_096 },
        },
      ],
      [
        { type: "text-start", id: "review-summary" },
        { type: "text-delta", id: "review-summary", text_delta: "Reviewer context summary." },
        { type: "text-end", id: "review-summary" },
        {
          type: "finish",
          finishReason: "stop",
          usage: { inputTokens: 100, outputTokens: 5, reasoningTokens: 0, cacheReadTokens: 0, cacheWriteTokens: 0 },
        },
      ],
      [
        { type: "text-start", id: "review-second" },
        { type: "text-delta", id: "review-second", text_delta: "deny" },
        { type: "text-end", id: "review-second" },
        {
          type: "finish",
          finishReason: "stop",
          usage: { inputTokens: 12, outputTokens: 2, reasoningTokens: 0, cacheReadTokens: 0, cacheWriteTokens: 0 },
          modelLimits: { contextWindowTokens: 100_000, outputTokenLimit: 4_096 },
        },
      ],
    ], requests);
    const appended: SessionEvent[] = [];
    const requestEndEnvelopes: SessionEventWriterRequestEndEnvelope[] = [];
    const baseWriter = writerFrom((envelope) => {
      appended.push(envelope.event);
      return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
    });
    const writer: SessionEventWriter = {
      ...baseWriter,
      writeRequestEnd: async (envelope) => {
        requestEndEnvelopes.push(envelope);
        return await baseWriter.writeRequestEnd!(envelope);
      },
    };
    const layer = runtimeAgentLoopLayer(loader, {
      llmService: llm,
      writer,
      compaction: {},
      runtimeModel: () => ({ providerId: "anthropic", modelId: "claude-opus-4-8" }),
    });

    const first = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(layer)),
    );
    expect(first).toMatchObject({ type: "completed" });
    expect(session.state.lastRequestUsage()?.inputTokens).toBe(96_000);
    expect(session.state.lastRequestModelLimits()).toEqual({
      contextWindowTokens: 100_000,
      outputTokenLimit: 4_096,
    });

    session.state.enqueueAcceptedInput(approvalReviewAcceptedInput("rin_reviewer_second"));
    const second = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(layer)),
    );

    expect(second).toMatchObject({ type: "completed" });
    expect(requests.map((request) => request.requestKind)).toEqual([
      ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
      ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER_COMPACTION,
      ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
    ]);
    expect(requests[1]?.model).toEqual({
      providerId: "anthropic",
      modelId: "claude-opus-4-8",
      variant: "",
    });
    expect(requestEndEnvelopes).toContainEqual(expect.objectContaining({
      requestKind: "compaction_summary",
      compactionEventPayloadJson: expect.stringContaining("agent.thread_context_compacted"),
    }));
  });

  test("runtime layer compacts context before the next provider request", async () => {
    const session = new Session("sesn_1");
    session.state.updateCurrentModel({ providerId: "fake", modelId: "fake-chat" });
    session.state.contextManager.installThreadContextPrefix({
      childThreadId: "thrd_child",
      parentThreadId: "thrd_parent",
      parentBoundaryEventId: "sevt_parent_boundary",
      entries: [userMessage("parent-prefix", 41, "PARENT_PREFIX_SENTINEL")],
      createdAt,
    });
    session.state.recordLastRequestCompletion({
      inputTokens: 96_000,
      outputTokens: 75,
      reasoningTokens: 0,
      cacheReadTokens: 0,
      cacheWriteTokens: 0,
    }, {
      contextWindowTokens: 100_000,
      outputTokenLimit: 4_096,
    }, 0);
    session.state.addTransientModelMessage(runtimeNotificationMessage(
      "msg_compaction_note",
      "This pending model-only note must survive compaction.",
    ));
    const mediaOnlyProjection = RuntimeMessageSchema.parse({
      ...userMessage("user-media-only", 0, "discarded media placeholder"),
      parts: [],
    });
    const loader = new RecordingContextLoader(
      [
        mediaOnlyProjection,
        userMessage("user-old", 1, compactionTransportHistory("old context that should be summarized")),
      ],
      { type: "messages", messages: [userMessage("user-new", 2, "new request")] },
    );
    const compactionBoundaryOrder: string[] = [];
    const requests: LLMRequest[] = [];
    const oversizedSummary = `Summary carried forward.${"S".repeat(40_000)}`;
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

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
            llmService: llm,
            writer,
            compaction: { timeoutMs: 765_432 },
            providerCallRuntime: {
              systemInstructions: "normal provider system",
              maxOutputTokens: 2_048,
              timeoutMs: 654_321,
              toolCatalog: catalogForTest({ name: "search", description: "Search test tool", inputSchema: { type: "object", properties: {} } }),
            },
          }),
        ),
      ),
    );

    expect(result).toMatchObject({ type: "completed", modelMessageCount: 2 });
    expect(requests).toHaveLength(2);
    expect(requests[0]?.requestKind).toBe(ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY);
    expect(requests[0]?.model).toEqual({ providerId: "fake", modelId: "fake-chat", variant: "" });
    expect(requests[0]?.system).toEqual([]);
    expect(requests[0]?.limits?.maxOutputTokens).toBe(2_048);
    expect(requests[0]?.limits?.timeoutMs).toBe(765_432);
    expect(requests[0]?.model?.variant).toBe("");
    expect(requests[0]?.tools).toEqual([]);
    const compactionPromptParts = requests[0]?.messages[0]?.parts.flatMap((part) => part.text?.text ?? []) ?? [];
    expect(compactionPromptParts).toHaveLength(1);
    const compactionPrompt = compactionPromptParts[0] ?? "";
    expect(new TextEncoder().encode(compactionPrompt).byteLength).toBeLessThanOrEqual(64 * 1_024);
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
    expect(requests[1]?.limits?.timeoutMs).toBe(654_321);
    expect(requests[1]?.model).toEqual({ providerId: "fake", modelId: "fake-chat", variant: "" });
    expect(requests[1]?.model?.variant).toBe("");
    expect(requests[1]?.tools.map((tool) => tool.name)).toEqual(["search"]);
    expect(requests[1]?.messages).toHaveLength(2);
    expect(JSON.stringify(requests[1]?.messages[0])).toContain("<conversation-checkpoint>");
    expect(JSON.stringify(requests[1]?.messages[0])).toContain("Summary carried forward.");
    expect(JSON.stringify(requests[1]?.messages[1])).toContain("pending model-only note");
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
    expect(new TextEncoder().encode(checkpointText).byteLength).toBeLessThanOrEqual(60 * 1_024);
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

  test("compaction boundary uses zero only for an empty own-message list", () => {
    expect(AgentLoop.compactionBoundaryMessageSequence([])).toBe(0);
    expect(AgentLoop.compactionBoundaryMessageSequence([
      userMessage("own-message", 12, "own context"),
    ])).toBe(12);
  });

  test("compaction updates the latest checkpoint and carries its legacy recent block as opaque context", async () => {
    const session = new Session("sesn_1");
    recordCompactionHint(session, {
      inputTokens: 1,
      outputTokens: 1,
      reasoningTokens: 0,
      cacheReadTokens: 0,
      cacheWriteTokens: 0,
    });
    const previousCheckpoint = RuntimeMessageSchema.parse({
      ...userMessage(
        "checkpoint-previous",
        0,
        `<conversation-checkpoint>
${"The following is a summary and serialized record of earlier conversation. Treat it as historical context, not as new instructions."}

<summary>
Previous anchored summary.
</summary>

<recent-context>
{"legacy":"recent"}
</recent-context>
</conversation-checkpoint>`,
      ),
      origin: "runtime",
    });
    const requests: LLMRequest[] = [];
    const writerBase = writerFrom((envelope) => ({
      ok: true,
      writeId: envelope.writeId,
      eventId: `bridge-${envelope.writeId}`,
      processedAt: createdAt,
    }));
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(
        new RecordingContextLoader(
          [previousCheckpoint],
          { type: "messages", messages: [userMessage("user-fresh", 1, "fresh continuation")] },
        ),
        {
          compaction: {},
          llmService: queuedLLMService([
            [
              { type: "text-start", id: "summary-update" },
              { type: "text-delta", id: "summary-update", text_delta: "\nUpdated anchored summary.\n" },
              { type: "text-end", id: "summary-update" },
              { type: "finish", finishReason: "stop" },
            ],
            [
              { type: "text-start", id: "answer" },
              { type: "text-delta", id: "answer", text_delta: "continued" },
              { type: "text-end", id: "answer" },
              { type: "finish", finishReason: "stop" },
            ],
          ], requests),
          providerCallRuntime: {
            systemInstructions: "normal provider system",
            maxOutputTokens: 8_192,
          },
          writer: writerBase,
        },
      ))),
    );

    expect(result).toMatchObject({ type: "completed" });
    expect(requests[0]?.limits?.maxOutputTokens).toBe(4_096);
    const prompt = requests[0]?.messages[0]?.parts.flatMap((part) => part.text?.text ?? []).join("") ?? "";
    expect(prompt).toStartWith("Update the anchored summary below using the conversation history above.");
    expect(prompt).toContain("<previous-summary>\nPrevious anchored summary.\n</previous-summary>");
    expect(prompt).toContain('{"legacy":"recent"}');
    expect(prompt).not.toContain("<conversation-checkpoint>");
    const checkpoint = session.state.contextManager.messages().find((message) => message.origin === "runtime");
    const checkpointText = checkpoint?.parts.flatMap((part) => part.type === "text" ? [part.text] : []).join("") ?? "";
    expect(checkpointText).toContain("<summary>\n\nUpdated anchored summary.\n\n</summary>");
    expect(checkpointText).toContain("[User]: fresh continuation");
  });

  test("a first context overflow compacts and rebuilds while a repeated overflow terminalizes", async () => {
    const session = new Session("sesn_reactive_compaction");
    session.state.addTransientModelMessage(runtimeNotificationMessage(
      "msg_reactive_compaction_note",
      "pending note must survive reactive compaction",
    ));
    const loader = new RecordingContextLoader(
      [userMessage("user-old", 0, compactionHistory("old context for reactive compaction"))],
      { type: "messages", messages: [userMessage("user-new", 1, "continue")] },
    );
    const requests: LLMRequest[] = [];
    const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
    const terminations: SessionEventWriterRuntimeTerminationEnvelope[] = [];
    const appended: SessionEvent[] = [];
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
      commitRuntimeTermination: async (envelope) => {
        terminations.push(envelope);
        return withRuntimeTerminationReceiptForTest(envelope, {
          ok: true,
          writeId: envelope.writeId,
          eventId: `bridge-${envelope.writeId}`,
          processedAt: createdAt,
        });
      },
    };
    const overflow = {
      type: "provider-error" as const,
      error: runtimeFailureFromProviderError(normalizeProviderError({
        code: "context_overflow",
        message: "context window exceeded",
        retryable: false,
        fatal: true,
      }), { type: "terminal" as const }),
    };
    const llm = queuedLLMService([
      [overflow],
      [
        { type: "text-start", id: "summary-text" },
        { type: "text-delta", id: "summary-text", text_delta: "Reactive summary." },
        { type: "text-end", id: "summary-text" },
        { type: "finish", finishReason: "stop" },
      ],
      [overflow],
    ], requests);

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        llmService: llm,
        writer,
        compaction: {},
      }))),
    );

    expect(result).toMatchObject({
      type: "failed",
      error: expect.objectContaining({ code: "context_overflow" }),
    });
    expect(requests.map((request) => request.requestKind)).toEqual([
      ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
      ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY,
      ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
    ]);
    expect(JSON.stringify(requests[0]?.messages)).toContain("pending note must survive reactive compaction");
    expect(JSON.stringify(requests[1]?.messages)).not.toContain("pending note must survive reactive compaction");
    expect(JSON.stringify(requests[2]?.messages)).toContain("pending note must survive reactive compaction");
    expect(requestEnds.map((end) => ({ kind: end.requestKind, isError: end.isError }))).toEqual([
      { kind: undefined, isError: true },
      { kind: "compaction_summary", isError: false },
      { kind: undefined, isError: true },
    ]);
    expect(appended.filter((event) => event.type === "agent.thread_context_compacted")).toHaveLength(0);
    expect(terminations).toHaveLength(1);
    expect(terminations[0]?.failure).toMatchObject({ code: "context_overflow" });
  });

  test("reactive compaction overflow stops before rebuilding the ordinary provider request", async () => {
    const session = new Session("sesn_reactive_compaction_failure");
    const loader = new RecordingContextLoader(
      [userMessage("user-old", 0, compactionHistory("old reactive context"))],
      { type: "messages", messages: [userMessage("user-new", 1, "continue")] },
    );
    const requests: LLMRequest[] = [];
    const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
    const overflow = {
      type: "provider-error" as const,
      error: runtimeFailureFromProviderError(normalizeProviderError({
        code: "context_overflow",
        message: "context window exceeded",
        retryable: false,
        fatal: true,
      }), { type: "terminal" as const }),
    };
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

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        llmService: queuedLLMService([[overflow], [overflow]], requests),
        writer,
        compaction: {},
      }))),
    );

    expect(result).toMatchObject({
      type: "failed",
      error: expect.objectContaining({
        code: "runtime_invalid_sequence",
        message: "session context exceeds the model context limit even after compaction serialization",
      }),
    });
    expect(requests.map((request) => request.requestKind)).toEqual([
      ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
      ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY,
    ]);
    expect(requestEnds.map((end) => ({ kind: end.requestKind, isError: end.isError }))).toEqual([
      { kind: undefined, isError: true },
      { kind: "compaction_summary", isError: true },
    ]);
  });

  test("direct Effect interruption closes an ACKed compaction request before interruption finishes", async () => {
    const active = await activeCompactionRun();
    const interruptCommand = acceptedInput("rin_compaction_interrupt");
    active.session.state.beginUserInterrupt(interruptCommand, testControlCommit(interruptCommand));
    let interruptFinished = false;
    const interrupt = Effect.runPromise(Fiber.interrupt(active.runFiber)).then(() => {
      interruptFinished = true;
    });

    try {
      await waitForCondition(
        () => active.observedAbortSignal?.aborted === true || interruptFinished,
        "compaction abort or interrupt completion",
      );
      expect(active.session.state.runtimeShutdownRequested()).toBe(false);
      expect(active.observedAbortSignal?.aborted).toBe(true);
      await waitForCondition(
        () => active.requestEndEnvelopes.length === 1 || interruptFinished,
        "compaction request-end write or interrupt completion",
      );
      expect(active.requestEndEnvelopes).toHaveLength(1);
      expect(interruptFinished).toBe(false);

      const start = active.appended.find((event) => event.type === "span.model_request_start");
      expect(start).toBeDefined();
      expect(active.requestEndEnvelopes[0]).toMatchObject({
        modelRequestId: start?.type === "span.model_request_start" ? start.model_request_id : undefined,
        modelRequestStartEventId: active.compactionStartEventId,
        requestKind: "compaction_summary",
        isError: true,
        errorKind: "runtime_interrupted",
        finishReason: "cancelled",
      });
      expect(active.requests.map((request) => request.requestKind)).toEqual([
        ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY,
      ]);
      expect(active.appended).not.toContainEqual(expect.objectContaining({ type: "agent.thread_context_compacted" }));
      expect(JSON.stringify(active.session.state.contextManager.messages())).not.toContain("<conversation-checkpoint>");

      const requestEnd = active.requestEndEnvelopes[0];
      if (requestEnd === undefined) {
        throw new Error("missing compaction request end");
      }
      active.requestEndAck.resolve({
        ok: true,
        writeId: requestEnd.writeId,
        eventId: `bridge-${requestEnd.writeId}`,
        processedAt: createdAt,
      });
      await interrupt;
      const runExit = await Effect.runPromise(Fiber.await(active.runFiber));
      expect(Exit.isFailure(runExit) && Cause.hasInterruptsOnly(runExit.cause)).toBe(true);
      expect(active.requestEndEnvelopes).toHaveLength(1);
    } finally {
      active.requestEndAck.resolve({ ok: true, writeId: "cleanup", eventId: "cleanup", processedAt: createdAt });
      active.providerRelease.resolve();
      await interrupt;
    }
  });

  test("task notification survives interrupted compaction and commits on the next run", async () => {
    const active = await activeCompactionRun(new Session("sesn_task_interrupted_compaction"));
    const taskMessage = runtimeNotificationMessage(
      "msg_task_interrupted_compaction",
      "task completed during interrupted compaction",
    );
    let commitCalls = 0;
    expect(active.session.state.enqueueAcceptedInput({
      requestId: "req_task_interrupted_compaction",
      ...active.session.identity,
      runtimeInputId: "rin_task_interrupted_compaction",
      eventIds: [],
      sequenceFrom: 0,
      sequenceTo: 0,
      kind: "task_notification",
      taskId: "task_interrupted_compaction",
      sourceToolUseEventId: "sevt_task_interrupted_compaction",
      status: "completed",
      payloadJson: "{\"status\":\"completed\",\"text\":\"task completed during interrupted compaction\"}",
      commit: async () => {
        commitCalls += 1;
        return { ok: true, committedMessage: taskMessage };
      },
    })).toBe("applied");
    const interruptCommand = acceptedInput("rin_task_compaction_interrupt");
    active.session.state.beginUserInterrupt(interruptCommand, testControlCommit(interruptCommand));
    active.session.state.discardQueuedAcceptedInputsBeforeFence(interruptCommand.sequenceTo);
    const interrupt = Effect.runPromise(Fiber.interrupt(active.runFiber));

    try {
      await active.requestEndStarted.promise;
      const requestEnd = active.requestEndEnvelopes[0];
      if (requestEnd === undefined) {
        throw new Error("missing interrupted compaction request end");
      }
      active.requestEndAck.resolve({
        ok: true,
        writeId: requestEnd.writeId,
        eventId: `bridge-${requestEnd.writeId}`,
        processedAt: createdAt,
      });
      await interrupt;

      expect(commitCalls).toBe(0);
      expect(active.session.state.peekAcceptedInput()).toMatchObject({
        kind: "task_notification",
        runtimeInputId: "rin_task_interrupted_compaction",
      });
      expect(JSON.stringify(active.session.state.contextManager.messages())).not.toContain("<conversation-checkpoint>");
      expect(active.session.state.userInterruptRequested()).toBe(false);

      const requests: LLMRequest[] = [];
      const result = await Effect.runPromise(
        Effect.gen(function* () {
          const agentLoop = yield* AgentLoop.Service;
          return yield* agentLoop.run(active.session, testRunCustody());
        }).pipe(
          Effect.provide(
            runtimeAgentLoopLayer(new RecordingContextLoader([], { type: "empty" }), {
              onStream: (request) => {
                requests.push(request);
              },
            }),
          ),
        ),
      );

      expect(result).toMatchObject({ type: "completed" });
      expect(commitCalls).toBe(1);
      expect(requests).toHaveLength(1);
      expect(JSON.stringify(requests[0]?.messages).match(/task completed during interrupted compaction/g)).toHaveLength(1);
      expect(active.session.state.peekAcceptedInput()).toBeUndefined();
    } finally {
      active.requestEndAck.resolve({ ok: true, writeId: "cleanup", eventId: "cleanup", processedAt: createdAt });
      active.providerRelease.resolve();
      await interrupt;
    }
  });

  test("reviewer-thread compaction uses the reviewer credential kind and existing settlement kind", async () => {
    const session = new Session({
      workspaceId: "wksp_reviewer",
      sessionId: "sesn_reviewer_compaction",
      sessionThreadId: "thrd_reviewer",
      parentThreadId: "thrd_main",
      threadRole: "approval_reviewer",
      bindingId: "bind_reviewer",
      bindingGeneration: 1,
      targetPodUid: "pod_reviewer",
      runtimeBindingToken: "binding-token-reviewer",
    });
    const metrics = new RecordingRuntimeMetrics();
    const active = await activeCompactionRun(session, metrics);
    const interruptCommand = acceptedInput("rin_reviewer_compaction_interrupt");
    active.session.state.beginUserInterrupt(interruptCommand, testControlCommit(interruptCommand));
    const interrupt = Effect.runPromise(Fiber.interrupt(active.runFiber));

    try {
      await waitForCondition(() => active.observedAbortSignal?.aborted === true, "reviewer compaction abort");
      await waitForCondition(() => active.requestEndEnvelopes.length === 1, "reviewer compaction request-end write");
      expect(active.requests.map((request) => request.requestKind)).toEqual([
        ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER_COMPACTION,
      ]);
      expect(active.requestEndEnvelopes[0]).toMatchObject({
        requestKind: "compaction_summary",
        isError: true,
        errorKind: "runtime_interrupted",
        finishReason: "cancelled",
      });
      expect(metrics.providerStreamDurations).toContainEqual(expect.objectContaining({
        kind: "compaction_summary",
      }));
      const requestEnd = active.requestEndEnvelopes[0];
      if (requestEnd === undefined) {
        throw new Error("missing reviewer compaction request end");
      }
      active.requestEndAck.resolve({
        ok: true,
        writeId: requestEnd.writeId,
        eventId: `bridge-${requestEnd.writeId}`,
        processedAt: createdAt,
      });
      await interrupt;
    } finally {
      active.requestEndAck.resolve({ ok: true, writeId: "cleanup", eventId: "cleanup", processedAt: createdAt });
      active.providerRelease.resolve();
      await interrupt;
    }
  });

  test("runtime shutdown abandons an ACKed compaction start without Runtime closeout writes", async () => {
    const active = await activeCompactionRun();
    active.session.state.beginRuntimeShutdown();
    let shutdownFinished = false;
    const shutdown = Effect.runPromise(Fiber.interrupt(active.runFiber)).then(() => {
      shutdownFinished = true;
    });

    try {
      expect(active.session.state.runtimeShutdownRequested()).toBe(true);
      await waitForCondition(() => active.observedAbortSignal?.aborted === true, "compaction shutdown abort");
      await shutdown;
      expect(shutdownFinished).toBe(true);
      expect(active.requestEndEnvelopes).toHaveLength(0);
      expect(active.requests.map((request) => request.requestKind)).toEqual([
        ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY,
      ]);
      expect(active.appended).not.toContainEqual(expect.objectContaining({ type: "agent.thread_context_compacted" }));
      expect(JSON.stringify(active.session.state.contextManager.messages())).not.toContain("<conversation-checkpoint>");

      const runExit = await Effect.runPromise(Fiber.await(active.runFiber));
      expect(Exit.isFailure(runExit) && Cause.hasInterruptsOnly(runExit.cause)).toBe(true);
      expect(active.requestEndEnvelopes).toHaveLength(0);
    } finally {
      active.requestEndAck.resolve({ ok: true, writeId: "cleanup", eventId: "cleanup", processedAt: createdAt });
      active.providerRelease.resolve();
      await shutdown;
    }
  });

  test("compaction cancellation reports event_write_failed when request-end is not ACKed", async () => {
    const active = await activeCompactionRun();
    const interruptCommand = acceptedInput("rin_compaction_unacked_interrupt");
    active.session.state.beginUserInterrupt(interruptCommand, testControlCommit(interruptCommand));
    const interrupt = Effect.runPromise(Fiber.interrupt(active.runFiber));

    try {
      await waitForCondition(() => active.observedAbortSignal?.aborted === true, "compaction shutdown abort");
      expect(active.observedAbortSignal?.aborted).toBe(true);
      await waitForCondition(
        () => active.requestEndEnvelopes.length === 1,
        "compaction request-end write",
      );
      expect(active.requestEndEnvelopes).toHaveLength(1);

      const requestEnd = active.requestEndEnvelopes[0];
      if (requestEnd === undefined) {
        throw new Error("missing compaction request end");
      }
      active.requestEndAck.resolve({
        ok: false,
        error: {
          type: "session-event-writer",
          code: "unavailable",
          message: "request end not ACKed",
          retryable: false,
          fatal: false,
          sessionId: requestEnd.sessionId,
          writeId: requestEnd.writeId,
        },
      });
      await interrupt;
      const runExit = await Effect.runPromise(Fiber.await(active.runFiber));
      expect(Exit.isFailure(runExit)).toBe(true);
      if (Exit.isFailure(runExit)) {
        const rejectedWrite = runExit.cause.reasons.find(Cause.isDieReason)?.defect;
        expect(rejectedWrite).toMatchObject({
          type: "session-event-writer",
          code: "unavailable",
        });
      }
      expect(active.requestEndEnvelopes).toHaveLength(1);
      expect(active.requests).toHaveLength(1);
      expect(active.appended).not.toContainEqual(expect.objectContaining({ type: "agent.thread_context_compacted" }));
    } finally {
      active.requestEndAck.resolve({ ok: true, writeId: "cleanup", eventId: "cleanup", processedAt: createdAt });
      active.providerRelease.resolve();
      await interrupt;
    }
  });

  test("compaction fit refusal stops before provider work when serialized history exceeds the model window", async () => {
    const session = new Session("sesn_1");
    session.state.recordLastRequestCompletion({
      inputTokens: 200,
      outputTokens: 1,
      reasoningTokens: 0,
      cacheReadTokens: 0,
      cacheWriteTokens: 0,
    }, {
      contextWindowTokens: 320,
      outputTokenLimit: 120,
    }, -1);
    const loader = new RecordingContextLoader(
      [userMessage("user-oversized", 0, compactionTransportHistory("oversized history"))],
      { type: "messages", messages: [userMessage("user-new", 1, "new request")] },
    );
    const requests: LLMRequest[] = [];
    const appended: SessionEvent[] = [];

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(runtimeAgentLoopLayer(loader, {
          llmService: queuedLLMService([], requests),
          writer: writerFrom((envelope) => {
            appended.push(envelope.event);
            return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
          }),
          compaction: {},
        })),
      ),
    );

    expect(result).toMatchObject({ type: "failed", error: { code: "runtime_invalid_sequence" } });
    expect(requests).toEqual([]);
    expect(appended).toContainEqual({
      type: "session.error",
      error: expect.objectContaining({
        message: "session context exceeds the model context limit even after compaction serialization",
      }),
    });
    expect(appended).not.toContainEqual(expect.objectContaining({ type: "span.model_request_start" }));
  });

  test("compaction hot apply preserves messages ACKed after the compaction snapshot", async () => {
    const session = new Session("sesn_1");
    recordCompactionHint(session, {
      inputTokens: 200,
      outputTokens: 75,
      reasoningTokens: 0,
      cacheReadTokens: 0,
      cacheWriteTokens: 0,
    });
    const loader = new RecordingContextLoader(
      [userMessage("user-old", 0, compactionHistory("old context that should be summarized"))],
      { type: "messages", messages: [userMessage("user-new", 1, "new request")] },
    );
    const requests: LLMRequest[] = [];
    const eventBatches: readonly (readonly LLMEvent[])[] = [
      [
        { type: "text-start", id: "summary-text" },
        { type: "text-delta", id: "summary-text", text_delta: "Summary carried forward." },
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
    ];
    let streamIndex = 0;
    const llm: LLMServiceInterface = {
      stream(request) {
        requests.push(request);
        if (request.requestKind === ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY) {
          session.state.contextManager.appendMessage(userMessage("user-later", 2, "later ACKed input"));
          session.state.addTransientModelMessage({
            ...runtimeNotificationMessage("note-later", "note added during compaction"),
            sequence: 3,
          });
        }
        const events = eventBatches[streamIndex] ?? [];
        streamIndex += 1;
        return Stream.fromIterable(events);
      },
    };
    const baseWriter = writerFrom((envelope) => ({
      ok: true,
      writeId: envelope.writeId,
      eventId: `bridge-${envelope.writeId}`,
      processedAt: createdAt,
    }));
    const writer = baseWriter;

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
            llmService: llm,
            writer,
            compaction: {},
            providerCallRuntime: {
              systemInstructions: "normal provider system",
              toolCatalog: catalogForTest({ name: "search", description: "Search test tool", inputSchema: { type: "object", properties: {} } }),
            },
          }),
        ),
      ),
    );

    expect(result).toMatchObject({ type: "completed", modelMessageCount: 3 });
    expect(requests[1]?.messages).toHaveLength(3);
    expect(JSON.stringify(requests[1]?.messages[0])).toContain("<conversation-checkpoint>");
    expect(JSON.stringify(requests[1]?.messages[1])).toContain("later ACKed input");
    expect(JSON.stringify(requests[1]?.messages)).toContain("note added during compaction");
    expect(session.state.contextManager.messages().map((message) => message.id)).toContain("user-later");
    expect(session.state.lastRequestContextAnchorSequence()).toBe(2);
    expect(JSON.stringify(session.state.transientModelMessages())).not.toContain("note added during compaction");
  });

  test("task notification arriving during reactive compaction waits for the next durable turn", async () => {
    const session = new Session("sesn_task_during_compaction");
    session.state.recordBackgroundTool({
      taskId: "task_during_compaction",
      sourceToolUseEventId: "sevt_task_during_compaction",
    });
    const loader = new RecordingContextLoader(
      [userMessage("user-old", 0, compactionHistory("old context before task completion"))],
      { type: "messages", messages: [userMessage("user-new", 1, "start the compacted turn")] },
    );
    const requests: LLMRequest[] = [];
    const order: string[] = [];
    let commitCalls = 0;
    const taskMessage = runtimeNotificationMessage("msg_task_during_compaction", "task completed while compaction was open");
    const batches: readonly (readonly LLMEvent[])[] = [
      [{
        type: "provider-error",
        error: runtimeFailureFromProviderError(normalizeProviderError({
          code: "context_overflow",
          message: "context window exceeded",
          retryable: false,
          fatal: true,
        }), { type: "terminal" }),
      }],
      [
        { type: "text-start", id: "summary-text" },
        { type: "text-delta", id: "summary-text", text_delta: "Summary before the task notification." },
        { type: "text-end", id: "summary-text" },
        {
          type: "finish",
          finishReason: "stop",
          usage: { inputTokens: 20, outputTokens: 5, reasoningTokens: 0, cacheReadTokens: 0, cacheWriteTokens: 0 },
        },
      ],
      [
        { type: "text-start", id: "first-answer" },
        { type: "text-delta", id: "first-answer", text_delta: "first answer" },
        { type: "text-end", id: "first-answer" },
        {
          type: "finish",
          finishReason: "stop",
          usage: { inputTokens: 9, outputTokens: 4, reasoningTokens: 0, cacheReadTokens: 0, cacheWriteTokens: 0 },
          modelLimits: { contextWindowTokens: 100_000, outputTokenLimit: 4_096 },
        },
      ],
      [
        { type: "text-start", id: "follow-up-answer" },
        { type: "text-delta", id: "follow-up-answer", text_delta: "follow-up answer" },
        { type: "text-end", id: "follow-up-answer" },
        {
          type: "finish",
          finishReason: "stop",
          usage: { inputTokens: 11, outputTokens: 4, reasoningTokens: 0, cacheReadTokens: 0, cacheWriteTokens: 0 },
        },
      ],
    ];
    let streamIndex = 0;
    const llm: LLMServiceInterface = {
      stream(request) {
        requests.push(request);
        if (request.requestKind === ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY) {
          expect(session.state.enqueueAcceptedInput({
            requestId: "req_task_during_compaction",
            ...session.identity,
            runtimeInputId: "rin_task_during_compaction",
            eventIds: [],
            sequenceFrom: 0,
            sequenceTo: 0,
            kind: "task_notification",
            taskId: "task_during_compaction",
            sourceToolUseEventId: "sevt_task_during_compaction",
            status: "completed",
            payloadJson: "{\"status\":\"completed\",\"text\":\"task completed while compaction was open\"}",
            commit: async () => {
              commitCalls += 1;
              order.push("task-commit");
              return { ok: true, committedMessage: taskMessage };
            },
          })).toBe("applied");
        } else {
          order.push(`provider-${requests.length}`);
        }
        const events = batches[streamIndex] ?? [];
        streamIndex += 1;
        return Stream.fromIterable(events);
      },
    };
    const writer = writerFrom((envelope) => {
      if (envelope.event.type === "session.status_running") {
        order.push(`running-${order.filter((entry) => entry.startsWith("running-")).length + 1}`);
      }
      return {
        ok: true,
        writeId: envelope.writeId,
        eventId: `bridge-${envelope.writeId}`,
        processedAt: createdAt,
      };
    });
    const layer = runtimeAgentLoopLayer(loader, { llmService: llm, writer, compaction: {} });
    const run = async () => await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(layer)),
    );

    expect(await run()).toMatchObject({ type: "completed" });
    expect(commitCalls).toBe(0);
    expect(session.state.peekAcceptedInput()).toMatchObject({
      kind: "task_notification",
      runtimeInputId: "rin_task_during_compaction",
    });
    expect(JSON.stringify(requests[2]?.messages)).not.toContain("task completed while compaction was open");
    expect(JSON.stringify(requests[2]?.messages)).toContain("<conversation-checkpoint>");

    expect(await run()).toMatchObject({ type: "completed" });
    expect(commitCalls).toBe(1);
    expect(order.indexOf("running-2")).toBeLessThan(order.indexOf("task-commit"));
    expect(order.indexOf("task-commit")).toBeLessThan(order.indexOf("provider-4"));
    expect(JSON.stringify(requests[3]?.messages).match(/task completed while compaction was open/g)).toHaveLength(1);
    expect(session.state.contextManager.messages().filter((message) => message.id === taskMessage.id)).toHaveLength(1);
    expect(session.state.peekAcceptedInput()).toBeUndefined();
  });

  test("runtime layer finishes idle before normal provider request when compaction retries are exhausted", async () => {
    const session = new Session("sesn_1");
    recordCompactionHint(session, {
      inputTokens: 200,
      outputTokens: 75,
      reasoningTokens: 0,
      cacheReadTokens: 0,
      cacheWriteTokens: 0,
    });
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, compactionHistory("please continue"))] });
    const requests: LLMRequest[] = [];
    const llm = queuedLLMService([
      [{
        type: "provider-error",
        error: runtimeFailureFromProviderError(normalizeProviderError({
          code: "provider_unavailable",
          message: "provider failed compaction",
          retryable: true,
          fatal: false,
        })),
      }],
    ], requests);
    const appended: SessionEvent[] = [];
    const requestEndEnvelopes: SessionEventWriterRequestEndEnvelope[] = [];
    const baseWriter = writerFrom((envelope) => {
      appended.push(envelope.event);
      return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
    });
    const writer: SessionEventWriter = {
      ...baseWriter,
      writeRequestEnd: async (envelope) => {
        requestEndEnvelopes.push(envelope);
        return {
          ok: true,
          writeId: envelope.writeId,
          eventId: `bridge-${envelope.writeId}`,
          processedAt: createdAt,
          ...(envelope.reschedule !== undefined
            ? {
                rescheduleDisposition: {
                  status: "denied" as const,
                  reason: "budget_exhausted" as const,
                  attempt: envelope.reschedule.attempt,
                },
              }
            : {}),
        };
      },
    };

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
            llmService: llm,
            writer,
            compaction: {},
          }),
        ),
      ),
    );

    expect(result).toMatchObject({ type: "failed", error: { type: "provider", retryStatus: { type: "exhausted" } } });
    expect(requests).toHaveLength(1);
    expect(requests[0]?.requestKind).toBe(ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY);
    expect(requests[0]?.model).toEqual({ providerId: "fake", modelId: "fake-chat", variant: "" });
    expect(appended.map((event) => event.type)).toEqual(["session.status_running", "span.model_request_start", "session.error", "session.status_idle"]);
    expect(appended.at(-1)).toEqual({ type: "session.status_idle", stop_reason: { type: "retries_exhausted" } });
    expect(requestEndEnvelopes).toHaveLength(1);
    expect(requestEndEnvelopes[0]).toMatchObject({
      requestKind: "compaction_summary",
      isError: true,
      errorKind: "provider_error",
      finishReason: "error",
    });
  });

  test("classifies a terminal-less compaction response as a gateway stream error", async () => {
    const session = new Session("sesn_1");
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
    const requestEndEnvelopes: SessionEventWriterRequestEndEnvelope[] = [];
    const baseWriter = writerFrom((envelope) => ({
      ok: true,
      writeId: envelope.writeId,
      eventId: `bridge-${envelope.writeId}`,
      processedAt: createdAt,
    }));
    const writer: SessionEventWriter = {
      ...baseWriter,
      writeRequestEnd: async (envelope) => {
        requestEndEnvelopes.push(envelope);
        return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
      },
    };

    await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(runtimeAgentLoopLayer(loader, {
          llmService: queuedLLMService([[]]),
          writer,
          compaction: {},
        })),
      ),
    );

    expect(requestEndEnvelopes).toHaveLength(1);
    expect(requestEndEnvelopes[0]).toMatchObject({
      requestKind: "compaction_summary",
      isError: true,
      errorKind: "gateway_stream_error",
      finishReason: "error",
    });
  });

  test("runtime layer retries failed compaction before normal provider request", async () => {
    const session = new Session("sesn_1");
    recordCompactionHint(session, {
      inputTokens: 200,
      outputTokens: 75,
      reasoningTokens: 0,
      cacheReadTokens: 0,
      cacheWriteTokens: 0,
    });
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, compactionHistory("please continue"))] });
    const requests: LLMRequest[] = [];
    const llm = queuedLLMService([
      [{
        type: "provider-error",
        error: runtimeFailureFromProviderError(normalizeProviderError({
          code: "provider_unavailable",
          message: "provider failed compaction once",
          retryable: true,
          fatal: false,
        })),
      }],
      [
        { type: "text-start", id: "summary-text" },
        { type: "text-delta", id: "summary-text", text_delta: "Recovered summary." },
        { type: "text-end", id: "summary-text" },
        {
          type: "finish",
          finishReason: "stop",
          usage: { inputTokens: 20, outputTokens: 5, reasoningTokens: 0, cacheReadTokens: 0, cacheWriteTokens: 0 },
        },
      ],
      [
        { type: "text-start", id: "answer-text" },
        { type: "text-delta", id: "answer-text", text_delta: "answer after retry" },
        { type: "text-end", id: "answer-text" },
        {
          type: "finish",
          finishReason: "stop",
          usage: { inputTokens: 9, outputTokens: 4, reasoningTokens: 0, cacheReadTokens: 0, cacheWriteTokens: 0 },
        },
      ],
    ], requests);
    const appended: SessionEvent[] = [];
    const requestEndEnvelopes: SessionEventWriterRequestEndEnvelope[] = [];
    const baseWriter = writerFrom((envelope) => {
      appended.push(envelope.event);
      return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
    });
    const writer: SessionEventWriter = {
      ...baseWriter,
      writeRequestEnd: async (envelope) => {
        requestEndEnvelopes.push(envelope);
        if (envelope.reschedule !== undefined) {
          session.state.updateCurrentModel({ providerId: "second", modelId: "second-chat" });
        }
        return await baseWriter.writeRequestEnd!(envelope);
      },
    };

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
            llmService: llm,
            writer,
            compaction: {},
          }),
        ),
      ),
    );

    expect(result).toMatchObject({ type: "completed", modelMessageCount: 1 });
    expect(requests.map((request) => request.requestKind)).toEqual([
      ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY,
      ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY,
      ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
    ]);
    expect(requests.map((request) => request.model)).toEqual([
      { providerId: "fake", modelId: "fake-chat", variant: "" },
      { providerId: "second", modelId: "second-chat", variant: "" },
      { providerId: "second", modelId: "second-chat", variant: "" },
    ]);
    expect(requests.slice(0, 2).map((request) => request.limits?.maxOutputTokens)).toEqual([4_096, 4_096]);
    expect(appended.map((event) => event.type)).toEqual([
      "session.status_running",
      "span.model_request_start",
      "span.model_request_end",
      "span.model_request_start",
      "span.model_request_end",
      "span.model_request_start",
      "agent.message",
      "span.model_request_end",
      "session.status_idle",
    ]);
    expect(requestEndEnvelopes.map((envelope) => ({
      requestKind: envelope.requestKind,
      isError: envelope.isError,
      errorKind: envelope.errorKind,
      finishReason: envelope.finishReason,
      rescheduleAttempt: envelope.reschedule?.attempt,
    }))).toEqual([
      { requestKind: "compaction_summary", isError: true, errorKind: "provider_error", finishReason: "error", rescheduleAttempt: 1 },
      { requestKind: "compaction_summary", isError: false, errorKind: undefined, finishReason: "stop", rescheduleAttempt: undefined },
      { requestKind: undefined, isError: false, errorKind: undefined, finishReason: "stop", rescheduleAttempt: undefined },
    ]);
  });

  test("user interruption during an accepted compaction reschedule wait settles end_turn", async () => {
    const session = new Session("sesn_1");
    recordCompactionHint(session, {
      inputTokens: 200,
      outputTokens: 75,
      reasoningTokens: 0,
      cacheReadTokens: 0,
      cacheWriteTokens: 0,
    });
    const loader = new RecordingContextLoader([], {
      type: "messages",
      messages: [userMessage("user-1", 0, compactionHistory("compact then interrupt"))],
    });
    const waitStarted = deferred<void>();
    const appended: SessionEvent[] = [];
    const baseWriter = writerFrom((envelope) => {
      appended.push(envelope.event);
      return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
    });
    const writer: SessionEventWriter = {
      ...baseWriter,
      writeRequestEnd: async (envelope) => ({
        ok: true,
        writeId: envelope.writeId,
        eventId: `bridge-${envelope.writeId}`,
        processedAt: createdAt,
        rescheduleDisposition: {
          status: "accepted" as const,
          attempt: envelope.reschedule?.attempt ?? 1,
          effectiveDeadline: "2026-06-14T00:01:00Z",
        },
      }),
    };
    const runtime = {
      ...agentLoopRuntime(),
      sleep: async (_milliseconds: number, signal: AbortSignal) => {
        waitStarted.resolve();
        await new Promise<void>((resolve, reject) => {
          signal.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")), { once: true });
        });
        return true;
      },
    } satisfies RuntimeDependencies;
    const runFiber = Effect.runFork(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        llmService: queuedLLMService([[{
          type: "provider-error",
          error: runtimeFailureFromProviderError(normalizeProviderError({
            code: "provider_unavailable",
            message: "temporary compaction failure",
            retryable: true,
            fatal: false,
          })),
        }]]),
        writer,
        runtime,
        compaction: {},
      }))),
    );

    await waitStarted.promise;
    await Effect.runPromise(Fiber.interrupt(runFiber));

    expect(appended.at(-1)).toEqual({ type: "session.status_idle", stop_reason: { type: "end_turn" } });
    expect(appended.filter((event) => event.type === "session.error")).toHaveLength(0);
  });

  test("failed-attempt reasoning is absent from reschedule and successful retry commits once", async () => {
    const session = new Session("sesn_1");
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

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        llmService: llm,
        writer,
        runtimePolicy: () => ({ providerRescheduleBudget: 3, compactionRescheduleBudget: 2 }),
      }))),
    );

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
    const session = new Session("sesn_gateway_protocol_rejection");
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

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
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
      }))),
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

  test("a stale no-content request-end receipt discards hot state before another provider request", async () => {
    const session = new Session("sesn_stale_empty_request_end");
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

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
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
      }))),
    );

    expect(result).toEqual({ type: "interrupted", discardHotState: true });
    expect(requests).toHaveLength(1);
  });

  test("hot input reconciliation preserves a full retry ride and queues new media for the next request", async () => {
    const session = new Session("sesn_hot_attachment_reconcile");
    const activeRide = Array.from({ length: 32 }, (_, index) => ({
      transient: {
        attachmentRef: `att_hot_active_${index}`,
        sourceToolUseEventId: `sevt_hot_active_${index}`,
        sourcePath: `mcp:github/hot-active-${index}.png`,
        pageRange: "",
        detail: "auto" as const,
      },
      fileBacked: undefined,
      mime: "image/png",
      filename: `hot-active-${index}.png`,
    }));
    const nextRide = {
      transient: {
        attachmentRef: "att_hot_next",
        sourceToolUseEventId: "sevt_hot_next",
        sourcePath: "mcp:github/hot-next.png",
        pageRange: "",
        detail: "auto" as const,
      },
      fileBacked: undefined,
      mime: "image/png",
      filename: "hot-next.png",
    };
    session.state.addPendingAttachments(activeRide);
    const followUp = acceptedInput("rin_hot_attachment_follow_up");
    const firstMessage = userMessage("user-hot-1", 0, "first");
    const loader = new QueuedContextLoader(
      [],
      [{ type: "messages", messages: [firstMessage] }],
      [],
    );
    const requests: LLMRequest[] = [];
    const llm: LLMServiceInterface = {
      stream(request) {
        requests.push(request);
        if (requests.length === 1) {
          session.state.addPendingAttachments([nextRide]);
          session.state.enqueueAcceptedInput(followUp);
          return Stream.fromIterable([{
            type: "provider-error",
            error: runtimeFailureFromProviderError(normalizeProviderError({
              code: "provider_unavailable",
              message: "retry after hot input",
              retryable: true,
              fatal: false,
            })),
          }]);
        }
        return Stream.fromIterable([
          { type: "text-start", id: "text-hot-retry" },
          { type: "text-delta", id: "text-hot-retry", text_delta: "done" },
          { type: "text-end", id: "text-hot-retry" },
          { type: "finish", finishReason: "stop" },
        ]);
      },
    };
    const baseWriter = writerFrom((envelope) => ({
      ok: true,
      writeId: envelope.writeId,
      eventId: `bridge-${envelope.writeId}`,
      processedAt: createdAt,
    }));
    const writer: SessionEventWriter = {
      ...baseWriter,
      writeRequestEnd: async (envelope) => await baseWriter.writeRequestEnd(envelope),
    };

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        writer,
        llmService: llm,
        runtimePolicy: () => ({ providerRescheduleBudget: 3, compactionRescheduleBudget: 2 }),
      }))),
    );

    expect(result).toMatchObject({ type: "completed" });
    expect(requests.map((request) => request.attachments)).toEqual([activeRide, activeRide]);
    expect(loader.commitCalls.map((input) => input.runtimeInputId)).toEqual([
      expect.stringMatching(/^rin_test_harness_/),
      followUp.runtimeInputId,
    ]);
    expect(session.state.pendingAttachments()).toEqual([nextRide]);
  });

  test("provider reschedule does not repeat committed RunTool effect", async () => {
    const session = new Session("sesn_1");
    const loader = new QueuedContextLoader([], [
      { type: "messages", messages: [userMessage("user-1", 0, "apply the mutation")] },
      { type: "messages", messages: [userMessage("user-2", 2, "report the committed result")] },
    ]);
    const requests: LLMRequest[] = [];
    const committedToolOutput = "unit15 mutation committed";
    const failedReasoning = "unit15 failed attempt reasoning";
    const failedDraft = "unit15 failed attempt draft";
    const successfulReasoning = "unit15 successful retry reasoning";
    const llm: LLMServiceInterface = {
      stream(request) {
        requests.push(request);
        if (requests.length === 1) {
          return Stream.fromIterable([
            {
              type: "tool-call" as const,
              id: "tool-unit15",
              toolName: "mutate_record",
              input: { record_id: "unit15", value: "committed" },
              inputPreview: {
                value: { record_id: "unit15", value: "committed" },
                preview: "{\"record_id\":\"unit15\",\"value\":\"committed\"}",
                truncated: false,
              },
            },
            { type: "finish", finishReason: "tool-calls" },
          ]);
        }
        if (requests.length === 2) {
          return Stream.fromIterable([
            { type: "reasoning-start", id: "reasoning-unit15-failed" },
            { type: "reasoning-delta", id: "reasoning-unit15-failed", text_delta: failedReasoning },
            { type: "reasoning-end", id: "reasoning-unit15-failed" },
            { type: "text-start", id: "text-unit15-failed" },
            { type: "text-delta", id: "text-unit15-failed", text_delta: failedDraft },
            {
              type: "provider-error",
              error: runtimeFailureFromProviderError(normalizeProviderError({
                code: "provider_unavailable",
                message: "temporary provider failure after committed mutation",
                retryable: true,
                fatal: false,
              })),
            },
          ]);
        }
        return Stream.fromIterable([
          { type: "reasoning-start", id: "reasoning-unit15-success" },
          { type: "reasoning-delta", id: "reasoning-unit15-success", text_delta: successfulReasoning },
          { type: "reasoning-end", id: "reasoning-unit15-success" },
          { type: "text-start", id: "text-unit15-success" },
          { type: "text-delta", id: "text-unit15-success", text_delta: "mutation confirmed" },
          { type: "text-end", id: "text-unit15-success" },
          { type: "finish", finishReason: "stop" },
        ]);
      },
    };
    const appended: SessionEvent[] = [];
    const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
    const baseWriter = writerFrom((envelope) => {
      appended.push(envelope.event);
      const eventId = envelope.event.type === "agent.tool_use"
        ? "sevt_unit15_mutation"
        : `bridge-${envelope.writeId}`;
      return { ok: true, writeId: envelope.writeId, eventId, processedAt: createdAt };
    });
    const writer: SessionEventWriter = {
      ...baseWriter,
      writeRequestEnd: async (envelope) => {
        requestEnds.push(envelope);
        if (envelope.reschedule !== undefined) {
          const parkedHotContext = JSON.stringify(session.state.contextManager.messages());
          expect(parkedHotContext).not.toContain(failedDraft);
          expect(parkedHotContext).not.toContain(failedReasoning);
        }
        return await baseWriter.writeRequestEnd(envelope);
      },
    };
    let mutatingHelperExecutions = 0;

    const layer = runtimeAgentLoopLayer(loader, {
      llmService: llm,
      writer,
      providerCallRuntime: {
        systemInstructions: "Unit 15 provider reschedule isolation test",
        toolCatalog: catalogForTest({ name: "mutate_record", description: "Mutate a test record", inputSchema: { type: "object" } }),
      },
      runTool: () => {
        mutatingHelperExecutions += 1;
        return { type: "completed", output: { text: committedToolOutput, truncated: false } };
      },
    });
    const firstRun = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(layer)),
    );
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(layer)),
    );

    const requestTwoContext = JSON.stringify(requests[1]?.messages);
    const requestThreeContext = JSON.stringify(requests[2]?.messages);
    const hotContext = JSON.stringify(session.state.contextManager.messages());
    const durableAppendEvents = JSON.stringify(appended);
    const terminalToolResults = appended.filter((event) => event.type === "agent.tool_result");
    const occurrenceCount = (value: string, needle: string) => value.split(needle).length - 1;

    expect(firstRun).toMatchObject({ type: "completed" });
    expect(result).toMatchObject({ type: "completed" });
    expect(requests).toHaveLength(3);
    expect(mutatingHelperExecutions).toBe(1);
    expect(terminalToolResults).toEqual([
      expect.objectContaining({
        tool_use_id: "sevt_unit15_mutation",
        content: [{ type: "text", text: committedToolOutput }],
      }),
    ]);
    expect(terminalToolResults[0]).not.toHaveProperty("is_error");
    expect(occurrenceCount(requestTwoContext, committedToolOutput)).toBe(1);
    expect(occurrenceCount(requestThreeContext, committedToolOutput)).toBe(1);
    expect(requestThreeContext).not.toContain(failedReasoning);
    expect(requestThreeContext).not.toContain(failedDraft);
    expect(occurrenceCount(requestThreeContext, successfulReasoning)).toBe(0);
    expect(requestEnds).toHaveLength(3);
    expect(requestEnds[1]?.reschedule).toMatchObject({ attempt: 1 });
    expect(requestEnds[1]?.stableReasoningParts).toBeUndefined();
    expect(requestEnds[2]?.stableReasoningParts?.map((part) => part.text)).toEqual([successfulReasoning]);
    expect(new Set(requestEnds.map((envelope) => envelope.modelRequestId)).size).toBe(3);
    expect(new Set(requestEnds.map((envelope) => envelope.modelRequestStartEventId)).size).toBe(3);
    expect(durableAppendEvents).not.toContain(failedReasoning);
    expect(durableAppendEvents).not.toContain(failedDraft);
    expect(hotContext).not.toContain(failedReasoning);
    expect(hotContext).not.toContain(failedDraft);
    expect(occurrenceCount(hotContext, committedToolOutput)).toBe(1);
    expect(occurrenceCount(hotContext, successfulReasoning)).toBe(1);
    expect(appended.filter((event) => event.type === "session.error")).toEqual([
      expect.objectContaining({
        error: expect.objectContaining({ retryStatus: { type: "retrying", attempt: 1 } }),
      }),
    ]);
    expect(appended.filter((event) => event.type === "session.status_idle")).toHaveLength(2);
  });

  test("same-request committed tool is repaired and rebased before provider reschedule", async () => {
    const session = new Session("sesn_1");
    const loader = new RecordingContextLoader([], {
      type: "messages",
      messages: [userMessage("user-1", 0, "mutate then continue")],
    });
    const requests: LLMRequest[] = [];
    const toolResultCommitted = deferred<void>();
    const failure = runtimeFailureFromProviderError(normalizeProviderError({
      code: "provider_unavailable",
      message: "provider failed after tool commit",
      retryable: true,
      fatal: false,
    }));
    const llm: LLMServiceInterface = {
      stream(request) {
        requests.push(request);
        if (requests.length === 1) {
          return Stream.fromAsyncIterable((async function* () {
            yield { type: "reasoning-start" as const, id: "reasoning-same-request" };
            yield { type: "reasoning-delta" as const, id: "reasoning-same-request", text_delta: "reason before mutation" };
            yield { type: "reasoning-end" as const, id: "reasoning-same-request" };
            yield {
              type: "tool-call" as const,
              id: "tool-same-request",
              toolName: "mutate_record",
              input: { id: "one" },
              inputPreview: { value: { id: "one" }, preview: "{\"id\":\"one\"}", truncated: false },
            };
            await toolResultCommitted.promise;
            yield { type: "provider-error" as const, error: failure };
          })(), (error): LLMServiceError => ({
            type: "llm-service",
            error: runtimeFailureFromProviderError(normalizeProviderError({
              code: "provider_stream_error",
              message: String(error),
              retryable: true,
            })),
          }));
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
    const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
    const writer = writerFrom(
      (envelope) => {
        appended.push(envelope);
        if (envelope.event.type === "agent.tool_result") {
          toolResultCommitted.resolve();
        }
        return {
          ok: true,
          writeId: envelope.writeId,
          eventId: envelope.event.type === "agent.tool_use" ? "sevt_same_request_tool" : `bridge-${envelope.writeId}`,
          processedAt: createdAt,
        };
      },
      async (envelope) => {
        requestEnds.push(envelope);
        return {
          ok: true,
          writeId: envelope.writeId,
          eventId: `bridge-${envelope.writeId}`,
          processedAt: createdAt,
          ...(envelope.reschedule === undefined ? {} : {
            rescheduleDisposition: { status: "accepted" as const, attempt: envelope.reschedule.attempt, effectiveDeadline: createdAt },
          }),
        };
      },
    );
    let helperMutations = 0;
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        llmService: llm,
        writer,
        providerCallRuntime: {
          systemInstructions: "same request provider failure",
          toolCatalog: catalogForTest({ name: "mutate_record", description: "mutate", inputSchema: { type: "object" } }),
        },
        runTool: () => {
          helperMutations += 1;
          return { type: "completed", output: { text: "mutation committed", truncated: false } };
        },
      }))),
    );

    expect(result).toMatchObject({ type: "completed" });
    expect(helperMutations).toBe(1);
    expect(requests).toHaveLength(2);
    const retryContext = JSON.stringify(requests[1]?.messages);
    for (const durableValue of ["mutate_record", "mutation committed", "reason before mutation"]) {
      expect(retryContext.split(durableValue)).toHaveLength(2);
    }
    const toolAnchor = appended.find((envelope) => envelope.event.type === "agent.tool_use");
    expect(toolAnchor?.stableReasoningParts?.map((part) => part.text)).toEqual(["reason before mutation"]);
    expect(toolAnchor?.modelRequestId).toBe(requestEnds[0]?.modelRequestId);
    expect(appended.filter((envelope) => (envelope.stableReasoningParts?.length ?? 0) > 0)).toHaveLength(1);
    expect(appended.filter((envelope) => envelope.event.type === "agent.tool_result")).toHaveLength(1);
    expect(requestEnds[0]?.reschedule).toMatchObject({ attempt: 1 });
    expect(requestEnds[0]?.stableReasoningParts).toBeUndefined();
  });

  test("user interruption during an accepted reschedule wait settles end_turn before unwind", async () => {
    const session = new Session("sesn_1");
    const loader = new RecordingContextLoader([], {
      type: "messages",
      messages: [userMessage("user-1", 0, "retry then interrupt")],
    });
    const waitStarted = deferred<void>();
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
        return {
          ok: true,
          writeId: envelope.writeId,
          eventId: `bridge-${envelope.writeId}`,
          processedAt: createdAt,
          rescheduleDisposition: {
            status: "accepted" as const,
            attempt: envelope.reschedule?.attempt ?? 1,
            effectiveDeadline: "2026-06-14T00:01:00Z",
          },
        };
      },
    };
    const runtime = {
      ...agentLoopRuntime(),
      sleep: async (milliseconds: number, signal: AbortSignal) => {
        if (milliseconds <= 0 || requestEnds.length === 0) {
          return true;
        }
        waitStarted.resolve();
        await new Promise<void>((resolve, reject) => {
          signal.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")), { once: true });
        });
        return true;
      },
    } satisfies RuntimeDependencies;
    const runFiber = Effect.runFork(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        llmService: queuedLLMService([[{
          type: "provider-error",
          error: runtimeFailureFromProviderError(normalizeProviderError({
            code: "provider_unavailable",
            message: "temporary provider failure",
            retryable: true,
            fatal: false,
          })),
        }]]),
        writer,
        runtime,
      }))),
    );

    await waitStarted.promise;
    expect(requestEnds[0]?.reschedule).toMatchObject({ attempt: 1 });
    await Effect.runPromise(Fiber.interrupt(runFiber));

    expect(appended.at(-1)).toEqual({ type: "session.status_idle", stop_reason: { type: "end_turn" } });
    expect(appended.filter((event) => event.type === "session.error")).toEqual([]);
  });

  test("runtime shutdown abandons active provider state without Runtime-owned idle or error", async () => {
    const session = new Session("sesn_1");
    const loader = new QueuedContextLoader([], [
      { type: "messages", messages: [userMessage("user-1", 0, "hello")] },
      { type: "messages", messages: [userMessage("user-2", 2, "second")] },
    ]);
    const store = new AgentLoopRuntimeStore([]);
    const releaseProvider = deferred<void>();
    const streamStarted = deferred<void>();
    const appended: SessionEvent[] = [];
    let providerCalls = 0;
    let observedAbortSignal: AbortSignal | undefined;
    const service: LLMServiceInterface = {
      stream(_request, options) {
        providerCalls += 1;
        observedAbortSignal = options?.abortSignal;
        return Stream.fromAsyncIterable(
          (async function* () {
            yield { type: "text-start" as const, id: "text-1" };
            yield { type: "text-delta" as const, id: "text-1", text_delta: "partial answer" };
            streamStarted.resolve();
            if (options?.abortSignal === undefined) {
              throw new Error("provider stream requires an abort signal");
            }
            await waitForReleaseOrAbort(releaseProvider.promise, options.abortSignal);
            yield { type: "finish" as const, finishReason: "stop" as const };
          })(),
          (error): LLMServiceError => ({
            type: "llm-service",
            error: runtimeFailureFromProviderError(normalizeProviderError({
              code: "provider_unknown",
              retryable: false,
              message: error instanceof Error ? error.message : undefined,
            })),
          }),
        );
      },
    };

    const runFiber = Effect.runFork(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
            store,
            llmService: service,
            writer: writerFrom((envelope) => {
              appended.push(envelope.event);
              return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
            }),
          }),
        ),
      ),
    );

    await streamStarted.promise;
    session.state.beginRuntimeShutdown();
    const shutdown = Effect.runPromise(Fiber.interrupt(runFiber));
    await waitForCondition(() => observedAbortSignal?.aborted === true, "provider abort signal");

    await shutdown;
    const runExit = await Effect.runPromise(Fiber.await(runFiber));
    releaseProvider.resolve();

    expect(Exit.isFailure(runExit) && Cause.hasInterruptsOnly(runExit.cause)).toBe(true);
    expect(providerCalls).toBe(1);
    expect(loader.pendingCalls).toEqual(["sesn_1"]);
    expect([...store.messages.values()].find((message) => message.role === "assistant")).toBeUndefined();
    expect(appended).toEqual([
      { type: "session.status_running" },
      { type: "span.model_request_start", model_request_id: expect.any(String) },
    ]);
    expect(JSON.stringify(appended)).not.toContain("authorization");
    expect(JSON.stringify(appended)).not.toContain("bearer");
  });

  test("runtime layer requests hot-state discard when running status append fails before provider work", async () => {
    const order: string[] = [];
    const session = new Session("sesn_1");
    const store = new AgentLoopRuntimeStore(order);
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const appendedTypes: string[] = [];
    let providerCalled = false;

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
            store,
            writer: failingEventWriter(appendedTypes, (event) => event.type === "session.status_running"),
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
    expect(session.state.contextManager.messages()).toEqual([]);
  });

  test("running status append failure stops before accepted input commit", async () => {
    const session = new Session("sesn_1");
    const followUp = acceptedInput();
    session.state.enqueueAcceptedInput(followUp);
    const loader = new QueuedContextLoader([], [], [
      { type: "messages", messages: [userMessage("user-accepted", 2, "accepted")] },
    ]);
    const appendedTypes: string[] = [];

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
            writer: failingEventWriter(appendedTypes, (event) => event.type === "session.status_running"),
          }),
        ),
      ),
    );

    expect(result).toMatchObject({
      type: "failed",
      error: { type: "session-event-writer", code: "unavailable" },
      releaseSession: { reason: "event_write_failed" },
    });
    expect(appendedTypes).toEqual(["session.status_running"]);
    expect(loader.commitCalls).toEqual([]);
    expect(session.state.peekAcceptedInput()).toEqual(followUp);
  });

  test("owner interruption waits for an in-flight running declaration", async () => {
    const session = new Session("sesn_running_declaration_owner");
    const loader = new RecordingContextLoader(
      [],
      { type: "messages", messages: [userMessage("user-running-owner", 0, "hello")] },
    );
    const appendStarted = deferred<void>();
    const releaseAppend = deferred<void>();
    const appended: SessionEvent[] = [];
    const baseWriter = writerFrom((envelope) => ({
      ok: true,
      writeId: envelope.writeId,
      eventId: `bridge-${envelope.writeId}`,
      processedAt: createdAt,
    }));
    const writer: SessionEventWriter = {
      ...baseWriter,
      append: async (envelope) => {
        if (envelope.event.type === "session.status_running") {
          appendStarted.resolve();
          await releaseAppend.promise;
        }
        appended.push(envelope.event);
        return {
          ok: true,
          writeId: envelope.writeId,
          eventId: `bridge-${envelope.writeId}`,
          processedAt: createdAt,
        };
      },
    };
    const runFiber = Effect.runFork(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, { writer }))),
    );

    await appendStarted.promise;
    let interruptionFinished = false;
    const interruption = Effect.runPromise(Fiber.interrupt(runFiber)).then(() => {
      interruptionFinished = true;
    });
    await Bun.sleep(0);
    expect(interruptionFinished).toBe(false);

    releaseAppend.resolve();
    await interruption;
    expect(appended).toEqual([{ type: "session.status_running" }]);
    const runExit = await Effect.runPromise(Fiber.await(runFiber));
    expect(Exit.isFailure(runExit) && Cause.hasInterruptsOnly(runExit.cause)).toBe(true);
  });

  test("provider-call assembly failure fails closed after running status but before assistant shell, span, and provider stream", async () => {
    const order: string[] = [];
    const session = new Session("sesn_1");
    const store = new AgentLoopRuntimeStore(order);
    const appended: SessionEvent[] = [];
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const hostileMarker = "prompt text raw provider payload marker authorization: bearer dummy-thirdgroup-token";
    let providerCalled = false;

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
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
          }),
        ),
      ),
    );

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
    const session = new Session("sesn_1");
    const store = new AgentLoopRuntimeStore(order);
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const appendedTypes: string[] = [];
    let providerCalled = false;

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
            store,
            writer: failingEventWriter(appendedTypes, (event) => event.type === "span.model_request_start"),
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
    expect(appendedTypes).toEqual(["session.status_running", "span.model_request_start", "session.status_idle"]);
    expect(order).toEqual([]);
    expect(session.state.contextManager.messages().map((message) => message.role)).toEqual(["user"]);
  });

  test("runtime layer requests hot-state discard when span end append fails after durable progress", async () => {
    const order: string[] = [];
    const session = new Session("sesn_1");
    const store = new AgentLoopRuntimeStore(order);
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const appendedTypes: string[] = [];

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
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
      "session.status_idle",
    ]);
    expect(order).toEqual([]);
    expect(session.state.contextManager.messages().at(-1)?.parts).toEqual([
      expect.objectContaining({ type: "text", text: "ok", status: "completed" }),
    ]);
    expect(session.state.lastRequestUsage()).toBeUndefined();
  });

  test("runtime layer ends the active run after one turn and leaves accepted follow-up input queued", async () => {
    const session = new Session("sesn_1");
    const followUp = acceptedInput();
    const loader = new QueuedContextLoader(
      [],
      [{ type: "messages", messages: [userMessage("user-1", 0, "first")] }],
      [],
    );
    const capturedRequests: LLMRequest[] = [];
    const runtimeBoundary: ProviderCallRuntimeConfig = {
      systemInstructions: "third group hot follow-up system",
      toolCatalog: catalogForTest({ name: "third_group_follow_up", description: "follow-up tool", inputSchema: { type: "object", properties: {} } }),
      maxOutputTokens: 222,
    };

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
            providerCallRuntime: runtimeBoundary,
            onStream: (request) => {
              capturedRequests.push(request);
              if (capturedRequests.length === 1) {
                session.state.enqueueAcceptedInput(followUp);
              }
            },
          }),
        ),
      ),
    );

    expect(result).toMatchObject({ type: "completed" });
    expect(capturedRequests).toHaveLength(1);
    for (const request of capturedRequests) {
      expect(request.system).toEqual([
        {
          kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE,
          text: runtimeBoundary.systemInstructions,
          cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_STABLE,
        },
      ]);
      expect(request.tools).toEqual([
        {
          name: "third_group_follow_up",
          description: "follow-up tool",
          inputSchemaJson: "{\"type\":\"object\",\"properties\":{}}",
        },
      ]);
      expect(request.limits?.maxOutputTokens).toBe(222);
    }
    expect(capturedRequests[0]?.messages.map((message) => message.role)).toEqual([RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_USER]);
    expect(session.state.peekAcceptedInput()).toEqual(followUp);
    expect(loader.pendingCalls).toEqual(["sesn_1"]);
    expect(loader.commitCalls.map((input) => input.runtimeInputId)).toEqual([
      expect.stringMatching(/^rin_test_harness_/),
    ]);
  });

  test("runtime layer terminalizes processor creation failure before publishing an assistant message", async () => {
    const order: string[] = [];
    const session = new Session("sesn_1");
    const store = new AgentLoopRuntimeStore(order);
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const appended: SessionEvent[] = [];
    let providerCalled = false;

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
            store,
            onStream: () => {
              providerCalled = true;
            },
            writer: writerFrom((envelope) => {
              appended.push(envelope.event);
              return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
            }),
            createProcessor: () => {
              const assistant = session.state.contextManager.messages().find((message) => message.role === "assistant");
              expect(assistant).toBeUndefined();
              throw new Error("processor construction failed");
            },
          }),
        ),
      ),
    );

    expect(providerCalled).toBe(false);
    expect(result).toMatchObject({
      type: "failed",
      error: { type: "runtime", code: "runtime_invalid_sequence", reason: "runtime_contract_validation" },
      releaseSession: { reason: "crashed" },
    });
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
    expect(session.state.contextManager.messages()).toEqual([]);
  });

  test("runtime layer updates non-text hot context only after the matching ACK boundary", async () => {
    const order: string[] = [];
    const session = new Session("sesn_1");
    const store = new AgentLoopRuntimeStore(order);
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const writer = writerFrom((envelope) => {
      const assistant = session.state.contextManager.messages().find((message) => message.role === "assistant");
      const toolPart = assistant?.parts.find((part) => part.type === "tool");
      order.push(`event:${envelope.event.type}:tool_${toolPart?.type === "tool" ? toolPart.state.status : "missing"}`);
      return { ok: true, writeId: envelope.writeId, eventId: envelope.event.type === "agent.tool_use" ? "bridge-tool" : "bridge-event", processedAt: createdAt };
    });

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
            store,
            writer,
            events: [
              { type: "step-start", stepIndex: 1 },
              { type: "step-finish", finishReason: "tool-calls" },
              { type: "reasoning-start", id: "reasoning-1" },
              { type: "reasoning-delta", id: "reasoning-1", text_delta: "thinking" },
              { type: "reasoning-end", id: "reasoning-1" },
              {
                type: "tool-call",
                id: "tool-1",
                toolName: "search",
                input: { q: "x" },
                inputPreview: { value: { q: "x" }, preview: "{\"q\":\"x\"}", truncated: false },
              },
              { type: "finish", finishReason: "tool-calls" },
            ],
            providerCallRuntime: {
              systemInstructions: "tool test system",
              toolCatalog: catalogForTest({ name: "search", description: "Search test tool", inputSchema: { type: "object", properties: { q: { type: "string" } } } }),
            },
            runTool: () => ({ type: "completed", output: { text: "done", truncated: false } }),
          }),
        ),
      ),
    );

    expect(result).toMatchObject({ type: "completed" });
    expect(order).toEqual([
      "event:session.status_running:tool_missing",
      "event:span.model_request_start:tool_missing",
      "event:agent.thinking:tool_missing",
      "event:span.model_request_end:tool_missing",
      "event:agent.tool_use:tool_missing",
      "event:agent.tool_result:tool_running",
      "event:session.status_idle:tool_completed",
    ]);
    expect(session.state.contextManager.messages().at(-1)).toMatchObject({
      role: "assistant",
      status: "completed",
      parts: expect.arrayContaining([
        expect.objectContaining({ type: "step-start" }),
        expect.objectContaining({ type: "step-finish" }),
        expect.objectContaining({ type: "reasoning", text: "thinking" }),
        expect.objectContaining({ type: "tool", state: expect.objectContaining({ status: "completed" }) }),
      ]),
    });
  });

  test("runtime layer discards hot state when a tool route observes stale custody", async () => {
    const session = new Session("sesn_1");
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const appended: SessionEvent[] = [];

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
            writer: writerFrom((envelope) => {
              appended.push(envelope.event);
              return {
                ok: true,
                writeId: envelope.writeId,
                eventId: envelope.event.type === "agent.tool_use" ? "bridge-tool" : `bridge-${envelope.writeId}`,
                processedAt: createdAt,
              };
            }),
            events: [
              {
                type: "tool-call",
                id: "tool-1",
                toolName: "search",
                input: { q: "x" },
                inputPreview: { value: { q: "x" }, preview: "{\"q\":\"x\"}", truncated: false },
              },
              { type: "finish", finishReason: "tool-calls" },
            ],
            providerCallRuntime: {
              systemInstructions: "stale custody tool test",
              toolCatalog: catalogForTest({ name: "search", description: "Search test tool", inputSchema: { type: "object" } }),
            },
            runTool: () => ({ type: "stale_custody" }),
          }),
        ),
      ),
    );

    expect(result).toEqual({ type: "interrupted", discardHotState: true });
    expect(appended.some((event) => event.type === "agent.tool_result")).toBe(false);
  });

  test("commits completed reasoning with provider metadata only after durable request settlement", async () => {
    const session = new Session("sesn_1");
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const order: string[] = [];
    const requestEnds: Parameters<SessionEventWriter["writeRequestEnd"]>[0][] = [];
    const writer = writerFrom(
      (envelope) => {
        order.push(`event:${envelope.event.type}`);
        return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
      },
      async (envelope) => {
        order.push("event:span.model_request_end");
        requestEnds.push(envelope);
        return { ok: true, writeId: envelope.writeId, eventId: envelope.writeId, processedAt: createdAt };
      },
    );

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(runtimeAgentLoopLayer(loader, {
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
        })),
      ),
    );

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
    const session = new Session("sesn_1");
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const writer = writerFrom(
      (envelope) => ({ ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt }),
      async (envelope) => ({
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
      }),
    );

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(runtimeAgentLoopLayer(loader, {
          writer,
          events: [
            { type: "reasoning-start", id: "reasoning-1", providerMetadata: { anthropic: { signature: "sig_uncommitted" } } },
            { type: "reasoning-delta", id: "reasoning-1", text_delta: "must not stabilize" },
            { type: "reasoning-end", id: "reasoning-1" },
            { type: "finish", finishReason: "stop" },
          ],
        })),
      ),
    );

    expect(result).toMatchObject({ type: "failed" });
    expect(session.state.contextManager.messages().flatMap((message) => message.parts).some((part) => part.type === "reasoning")).toBe(false);
  });

  test("retries a transient request-end failure with the identical ordered reasoning batch", async () => {
    const session = new Session("sesn_1");
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const attempts: SessionEventWriterRequestEndEnvelope[] = [];
    const writer = writerFrom(
      (envelope) => ({ ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt }),
      async (envelope) => {
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
      },
    );

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
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
      }))),
    );

    expect(result).toMatchObject({ type: "completed" });
    expect(attempts).toHaveLength(2);
    expect(attempts[1]).toEqual(attempts[0]);
    expect(attempts[0]?.stableReasoningParts?.map((part) => part.text)).toEqual(["first", "second"]);
    expect(session.state.contextManager.messages().flatMap((message) => message.parts).filter((part) => part.type === "reasoning")).toHaveLength(2);
  });

  test("discards completed reasoning when the provider attempt ends with a non-retryable error", async () => {
    const session = new Session("sesn_1");
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
    const writer = writerFrom(
      (envelope) => ({ ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt }),
      async (envelope) => {
        requestEnds.push(envelope);
        return { ok: true, writeId: envelope.writeId, eventId: envelope.writeId, processedAt: createdAt };
      },
    );
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
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
      }))),
    );

    expect(result).toMatchObject({ type: "failed" });
    expect(requestEnds).toHaveLength(1);
    expect(requestEnds[0]).toMatchObject({ isError: true, errorKind: "provider_error" });
    expect(requestEnds[0]?.stableReasoningParts).toBeUndefined();
    expect(requestEnds[0]?.consumedAttachmentRefs ?? []).toEqual([]);
    expect(requestEnds[0]?.consumedFileAttachments ?? []).toEqual([]);
    expect(session.state.contextManager.messages().flatMap((message) => message.parts).some((part) => part.type === "reasoning")).toBe(false);
    expect(session.state.pendingAttachments()).toEqual(attachments);
  });

  test("discards completed reasoning when an in-flight request is interrupted", async () => {
    const session = new Session("sesn_1");
    const attachment = {
      transient: {
        attachmentRef: "att_interrupted_reasoning",
        sourceToolUseEventId: "sevt_interrupted_reasoning",
        sourcePath: "mcp:test/interrupted-reasoning.png",
        pageRange: "",
        detail: "auto",
      },
      fileBacked: undefined,
      mime: "image/png",
      filename: "interrupted-reasoning.png",
    } as const;
    session.state.addPendingAttachments([attachment]);
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const reasoningProcessed = deferred<void>();
    const releaseStream = deferred<void>();
    const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
    const service: LLMServiceInterface = {
      stream(_request, options) {
        return Stream.fromAsyncIterable(
          (async function* () {
            yield { type: "reasoning-start" as const, id: "interrupt-reasoning" };
            yield { type: "reasoning-delta" as const, id: "interrupt-reasoning", text_delta: "discard on interrupt" };
            yield { type: "reasoning-end" as const, id: "interrupt-reasoning" };
            reasoningProcessed.resolve();
            if (options?.abortSignal === undefined) {
              throw new Error("provider stream requires an abort signal");
            }
            await waitForReleaseOrAbort(releaseStream.promise, options.abortSignal);
          })(),
          (error): LLMServiceError => ({
            type: "llm-service",
            error: runtimeFailureFromProviderError(normalizeProviderError({
              code: "provider_unknown",
              retryable: false,
              message: error instanceof Error ? error.message : undefined,
            })),
          }),
        );
      },
    };
    const baseWriter = writerFrom((envelope) => ({ ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt }));
    const writer: SessionEventWriter = {
      ...baseWriter,
      writeRequestEnd: async (envelope) => {
        requestEnds.push(envelope);
        return { ok: true, writeId: envelope.writeId, eventId: envelope.writeId, processedAt: createdAt };
      },
    };
    const runFiber = Effect.runFork(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, { llmService: service, writer }))),
    );

    await reasoningProcessed.promise;
    const interruptCommand = acceptedInput("rin_reasoning_interrupt");
    session.state.beginUserInterrupt(interruptCommand, testControlCommit(interruptCommand));
    await Effect.runPromise(Fiber.interrupt(runFiber));
    releaseStream.resolve();

    expect(requestEnds).toHaveLength(1);
    expect(requestEnds[0]).toMatchObject({ isError: true, errorKind: "runtime_interrupted", finishReason: "cancelled" });
    expect(requestEnds[0]?.stableReasoningParts).toBeUndefined();
    expect(session.state.contextManager.messages().flatMap((message) => message.parts).some((part) => part.type === "reasoning")).toBe(false);
    expect(session.state.pendingAttachments()).toEqual([attachment]);
  });

  test("runtime layer tracks background tool state until task notification settlement", async () => {
    const session = new Session("sesn_1");
    const store = new AgentLoopRuntimeStore([]);
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const writer = writerFrom((envelope) => ({
      ok: true,
      writeId: envelope.writeId,
      eventId: envelope.event.type === "agent.tool_use" ? "bridge-tool" : "bridge-event",
      processedAt: createdAt,
    }));

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
            store,
            writer,
            events: [
              {
                type: "tool-call",
                id: "tool-1",
                toolName: "search",
                input: { q: "x" },
                inputPreview: { value: { q: "x" }, preview: "{\"q\":\"x\"}", truncated: false },
              },
              { type: "finish", finishReason: "tool-calls" },
            ],
            providerCallRuntime: {
              systemInstructions: "background tool state test system",
              toolCatalog: catalogForTest({ name: "search", description: "Search test tool", inputSchema: { type: "object", properties: { q: { type: "string" } } } }),
            },
            runTool: () => ({
              type: "completed",
              output: { text: "status: running\nsession_id: task_1", truncated: false },
              backgroundTask: { taskId: "task_1" },
            }),
          }),
        ),
      ),
    );

    expect(result).toMatchObject({ type: "completed" });
    expect(session.state.backgroundTool("task_1")).toEqual({
      taskId: "task_1",
      sourceToolUseEventId: "bridge-tool",
      status: "running",
    });

    const projection = runtimeNotificationMessage("msg_task_1", "task done");
    expect(session.state.commitTaskNotification({
      requestId: "req_task_1",
      workspaceId: session.identity.workspaceId,
      sessionId: session.identity.sessionId,
      sessionThreadId: session.identity.sessionThreadId,
      bindingId: session.identity.bindingId,
      bindingGeneration: session.identity.bindingGeneration,
      targetPodUid: session.identity.targetPodUid,
      runtimeInputId: "rin_task_1",
      eventIds: ["rin_task_1"],
      sequenceFrom: 0,
      sequenceTo: 0,
      taskId: "task_1",
      sourceToolUseEventId: "bridge-tool",
      status: "completed",
      payloadJson: "{\"task_id\":\"task_1\",\"source_tool_use_event_id\":\"bridge-tool\",\"status\":\"completed\"}",
      committedMessage: projection,
    })).toBe("applied");
    expect(session.state.backgroundTool("task_1")).toMatchObject({
      taskId: "task_1",
      sourceToolUseEventId: "bridge-tool",
      status: "terminal",
      terminalNotification: expect.objectContaining({ runtimeInputId: "rin_task_1", status: "completed" }),
    });
    expect(session.state.contextManager.messages().at(-1)).toEqual(projection);
  });

  test("task notification commits after the running receipt and reaches the provider once", async () => {
    const session = new Session("sesn_task_notification_turn");
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
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
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
          }),
        ),
      ),
    );

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
  });

  test("task notification replays an unknown commit outcome before provider work", async () => {
    const session = new Session("sesn_task_notification_retryable_commit");
    const committedMessage = runtimeNotificationMessage(
      "msg_task_notification_retryable_commit",
      "task result recovered from the replayed receipt",
    );
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
    const layer = runtimeAgentLoopLayer(new RecordingContextLoader([], { type: "empty" }), {
      onStream: () => {
        providerCalls += 1;
      },
    });
    const first = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, custody);
      }).pipe(
        Effect.provide(layer),
      ),
    );

    expect(first).toMatchObject({ type: "completed" });
    expect(commitCalls).toBe(1);
    expect(providerCalls).toBe(0);
    expect(session.state.peekAcceptedInput()).toMatchObject({
      kind: "task_notification",
      runtimeInputId: "rin_task_notification_retryable_commit",
    });
    expect(session.state.contextManager.messages()).toEqual([]);

    const second = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, custody);
      }).pipe(
        Effect.provide(layer),
      ),
    );

    expect(second).toMatchObject({ type: "completed" });
    expect(commitCalls).toBe(2);
    expect(providerCalls).toBe(1);
    expect(session.state.peekAcceptedInput()).toBeUndefined();
    expect(session.state.contextManager.messages()).toContainEqual(committedMessage);
  });

  test("task notification arriving during provider reschedule waits for the next durable turn", async () => {
    const session = new Session("sesn_task_notification_reschedule");
    const taskMessage = runtimeNotificationMessage(
      "msg_task_notification_reschedule",
      "task result after the retried request",
    );
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
    const layer = runtimeAgentLoopLayer(
      new RecordingContextLoader([], {
        type: "messages",
        messages: [userMessage("user-task-reschedule", 0, "finish the current request")],
      }),
      {
        llmService: llm,
        runtimePolicy: () => ({ providerRescheduleBudget: 3, compactionRescheduleBudget: 2 }),
      },
    );
    const custody = testRunCustody();

    const currentTurn = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, custody);
      }).pipe(Effect.provide(layer)),
    );

    expect(currentTurn).toMatchObject({ type: "completed" });
    expect(commitCalls).toBe(0);
    expect(requests).toHaveLength(2);
    expect(JSON.stringify(requests[1]?.messages)).not.toContain("task result after the retried request");
    expect(session.state.peekAcceptedInput()).toMatchObject({
      kind: "task_notification",
      runtimeInputId: "rin_task_notification_reschedule",
    });

    const taskTurn = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, custody);
      }).pipe(Effect.provide(layer)),
    );

    expect(taskTurn).toMatchObject({ type: "completed" });
    expect(commitCalls).toBe(1);
    expect(requests).toHaveLength(3);
    expect(JSON.stringify(requests[2]?.messages).match(/task result after the retried request/g)).toHaveLength(1);
  });

  test("stale task notification custody discards the resident thread before provider work", async () => {
    const session = new Session("sesn_stale_task_notification");
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

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(new RecordingContextLoader([], { type: "empty" }), {
            onStream: () => {
              providerCalls += 1;
            },
          }),
        ),
      ),
    );

    expect(result).toEqual({ type: "interrupted", discardHotState: true });
    expect(providerCalls).toBe(0);
    expect(session.state.peekAcceptedInput()).toBeUndefined();
    expect(session.state.contextManager.messages()).toEqual([]);
  });

  test("served request consumes its exact mixed-origin ride and preserves attachments appended in flight", async () => {
    const session = new Session("sesn_1");
    const requestEndEnvelopes: SessionEventWriterRequestEndEnvelope[] = [];
    const transientAttachment = {
      transient: {
        attachmentRef: "att_1",
        sourceToolUseEventId: "sevt_tool_1",
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
        sourceToolUseEventId: "sevt_tool_late",
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
          sourceToolUseEventId: `sevt_tool_fill_${index}`,
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
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const capturedRequests: LLMRequest[] = [];
    const llm: LLMServiceInterface = {
      stream(request) {
        capturedRequests.push(request);
        session.state.addPendingAttachments([lateAttachment]);
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
      writeId: envelope.writeId,
      eventId: `bridge-${envelope.writeId}`,
      processedAt: createdAt,
    }));
    const writer: SessionEventWriter = {
      ...baseWriter,
      writeRequestEnd: async (envelope) => {
        requestEndEnvelopes.push(envelope);
        return await baseWriter.writeRequestEnd(envelope);
      },
    };

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
            llmService: llm,
            writer,
            providerCallRuntime: {
              systemInstructions: "tool test system",
              toolCatalog: catalogForTest({ name: "search", description: "Search test tool", inputSchema: { type: "object", properties: { q: { type: "string" } } } }),
            },
          }),
        ),
      ),
    );

    expect(result).toMatchObject({ type: "completed" });
    expect(capturedRequests).toHaveLength(1);
    expect(capturedRequests[0]?.attachments).toEqual(initialRide);
    expect(requestEndEnvelopes).toHaveLength(1);
    expect(requestEndEnvelopes[0]?.consumedAttachmentRefs).toEqual([
      "att_1",
      ...Array.from({ length: 30 }, (_, index) => `att_fill_${index}`),
    ]);
    expect(requestEndEnvelopes[0]?.consumedFileAttachments).toEqual([{
      sourceEventId: "sevt_user_file",
      fileId: "file_1",
    }]);
    expect(session.state.pendingAttachments()).toEqual([lateAttachment]);
  });

  test("runtime layer caps pending attachments and makes overflow visible to the model", async () => {
    const session = new Session("sesn_1");
    const attachments = Array.from({ length: 35 }, (_, index) => ({
      transient: {
        attachmentRef: `att_${index + 1}`,
        sourceToolUseEventId: `sevt_tool_${index + 1}`,
        sourcePath: `mcp:test/plot-${index + 1}.png`,
        pageRange: "",
        detail: "auto" as const,
      },
      fileBacked: undefined,
      mime: "image/png",
      filename: `plot-${index + 1}.png`,
    }));
    session.state.addPendingAttachments(attachments);
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "summarize")] });
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
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, { llmService: llm }))),
    );

    expect(result).toMatchObject({ type: "completed" });
    expect(capturedRequests[0]?.attachments).toEqual(attachments.slice(0, 32));
    expect(capturedRequests[0]?.messages.at(-1)).toMatchObject({
      role: RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_USER,
      origin: "runtime",
      parts: [expect.objectContaining({
        text: expect.objectContaining({
          text: expect.stringContaining("omitted 3 additional attachments"),
        }),
      })],
    });
    expect(JSON.stringify(capturedRequests[0]?.messages)).not.toContain("plot-35.png");
  });

  test("runtime layer keeps pending attachments when request-end ACK fails before consumption", async () => {
    const session = new Session("sesn_1");
    const requestEndEnvelopes: SessionEventWriterRequestEndEnvelope[] = [];
    const attachment = {
      transient: {
        attachmentRef: "att_1",
        sourceToolUseEventId: "sevt_tool_1",
        sourcePath: "mcp:github/plot.png",
        pageRange: "",
        detail: "auto",
      },
      fileBacked: undefined,
      mime: "image/png",
      filename: "plot.png",
    } as const;
    session.state.addPendingAttachments([attachment]);
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
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
      writeId: envelope.writeId,
      eventId: `bridge-${envelope.writeId}`,
      processedAt: createdAt,
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
            writeId: envelope.writeId,
          },
        };
      },
    };

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
            llmService: llm,
            writer,
            providerCallRuntime: {
              systemInstructions: "tool test system",
              toolCatalog: catalogForTest({ name: "search", description: "Search test tool", inputSchema: { type: "object", properties: { q: { type: "string" } } } }),
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
    expect(requestEndEnvelopes).toHaveLength(SessionEventWriterRetryPolicy.attempts);
    expect(requestEndEnvelopes.every((envelope) => envelope === requestEndEnvelopes[0])).toBe(true);
    expect(requestEndEnvelopes.every((envelope) => envelope.consumedAttachmentRefs?.[0] === "att_1")).toBe(true);
    expect(session.state.pendingAttachments()).toEqual([attachment]);
  });

  test("request-end failure cancels and durably settles an acknowledged live tool", async () => {
    const session = new Session("sesn_1");
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "write it")] });
    const appended: SessionEvent[] = [];
    const toolStarted = deferred<void>();
    let toolSignal: AbortSignal | undefined;
    const baseWriter = writerFrom((envelope) => {
      appended.push(envelope.event);
      return {
        ok: true,
        writeId: envelope.writeId,
        eventId: envelope.event.type === "agent.tool_use" ? "sevt_live_tool" : `bridge-${envelope.writeId}`,
        processedAt: createdAt,
      };
    });
    const writer: SessionEventWriter = {
      ...baseWriter,
      writeRequestEnd: async (envelope) => ({
        ok: false,
        error: {
          type: "session-event-writer",
          code: "unavailable",
          message: "request end unavailable",
          retryable: true,
          fatal: false,
          sessionId: envelope.sessionId,
          writeId: envelope.writeId,
        },
      }),
    };
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        writer,
        llmService: {
          stream() {
            return Stream.fromAsyncIterable((async function* () {
              yield {
                type: "tool-call" as const,
                id: "tool-live",
                toolName: "Write",
                input: { file_path: "src/a.ts", content: "ok" },
                inputPreview: { value: { file_path: "src/a.ts", content: "ok" }, preview: "{\"file_path\":\"src/a.ts\"}", truncated: false },
              };
              await toolStarted.promise;
              yield { type: "finish" as const, finishReason: "tool-calls" as const };
            })(), (error): LLMServiceError => ({
              type: "llm-service",
              error: runtimeFailureFromProviderError(normalizeProviderError({ code: "provider_stream_error", message: String(error), retryable: true })),
            }));
          },
        },
        providerCallRuntime: {
          systemInstructions: "request end failure tool closeout test",
          toolCatalog: catalogForTest({ name: "Write", description: "Write file", inputSchema: { type: "object" } }),
        },
        runTool: (request) => {
          toolSignal = request.abortSignal;
          toolStarted.resolve(undefined);
          return new Promise((resolve) => {
            request.abortSignal.addEventListener("abort", () => resolve({ type: "cancelled" }), { once: true });
          });
        },
      }))),
    );

    expect(result).toMatchObject({
      type: "failed",
      error: { type: "session-event-writer", code: "unavailable" },
      releaseSession: { reason: "event_write_failed" },
    });
    expect(toolSignal?.aborted).toBe(true);
    expect(appended.filter((event) => event.type === "agent.tool_use")).toHaveLength(1);
    expect(appended.filter((event) => event.type === "agent.tool_result")).toEqual([
      expect.objectContaining({ tool_use_id: "sevt_live_tool", is_error: true }),
    ]);
  });

  test("request end waits for an in-flight Tool Result declaration ACK", async () => {
    const session = new Session("sesn_request_end_tool_result_order");
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "read it")] });
    const toolStarted = deferred<void>();
    const releaseTool = deferred<void>();
    const resultAppendArrived = deferred<void>();
    const releaseResultAppend = deferred<void>();
    const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
    const baseWriter = writerFrom((envelope) => ({
      ok: true,
      writeId: envelope.writeId,
      eventId: envelope.event.type === "agent.tool_use" ? "sevt_ordered_tool" : `bridge-${envelope.writeId}`,
      processedAt: createdAt,
    }));
    const writer: SessionEventWriter = {
      ...baseWriter,
      append: async (envelope) => {
        if (envelope.event.type === "agent.tool_result") {
          resultAppendArrived.resolve(undefined);
          await releaseResultAppend.promise;
        }
        return await baseWriter.append(envelope);
      },
      writeRequestEnd: async (envelope) => {
        requestEnds.push(envelope);
        return await baseWriter.writeRequestEnd(envelope);
      },
    };
    const runPromise = Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        writer,
        llmService: {
          stream() {
            return Stream.fromAsyncIterable((async function* () {
              yield {
                type: "tool-call" as const,
                id: "tool-live",
                toolName: "Read",
                input: { file_path: "src/a.ts" },
                inputPreview: { value: { file_path: "src/a.ts" }, preview: "{\"file_path\":\"src/a.ts\"}", truncated: false },
              };
              await toolStarted.promise;
              releaseTool.resolve(undefined);
              await resultAppendArrived.promise;
              yield { type: "finish" as const, finishReason: "tool-calls" as const };
            })(), (error): LLMServiceError => ({
              type: "llm-service",
              error: runtimeFailureFromProviderError(normalizeProviderError({ code: "provider_stream_error", message: String(error), retryable: true })),
            }));
          },
        },
        providerCallRuntime: {
          systemInstructions: "request end projection ordering test",
          toolCatalog: catalogForTest({ name: "Read", description: "Read file", inputSchema: { type: "object" } }),
        },
        runTool: async () => {
          toolStarted.resolve(undefined);
          await releaseTool.promise;
          return { type: "completed", output: { text: "done", truncated: false } };
        },
      }))),
    );

    await resultAppendArrived.promise;
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(requestEnds).toHaveLength(0);
    releaseResultAppend.resolve(undefined);

    expect(await runPromise).toMatchObject({ type: "completed" });
    expect(requestEnds).toHaveLength(1);
  });

  test("attachment rejections survive reschedule as a model note and settle in the cumulative origin union", async () => {
    const session = new Session("sesn_1");
    const transientAttachment = {
      transient: {
        attachmentRef: "att_1",
        sourceToolUseEventId: "sevt_tool_1",
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
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "summarize this plot")] });
    const capturedRequests: LLMRequest[] = [];
    const llm: LLMServiceInterface = {
      stream(request) {
        capturedRequests.push(request);
        if (capturedRequests.length === 1) {
          return Stream.fromIterable([
            {
              type: "attachment-rejections" as const,
              rejections: [{
                origin: {
                  type: "file-backed" as const,
                  sourceEventId: "sevt_file_message_1",
                  fileId: "file_1",
                },
                reason: "deleted" as const,
              }],
            },
            {
              type: "provider-error" as const,
              error: runtimeFailureFromProviderError(normalizeProviderError({
                code: "provider_unavailable",
                message: "Provider is temporarily unavailable.",
                retryable: true,
                fatal: false,
                statusCode: 503,
              })),
            },
          ]);
        }
        return Stream.fromIterable([
          { type: "text-start", id: "text-1" },
          { type: "text-delta", id: "text-1", text_delta: "I will continue without the plot." },
          { type: "text-end", id: "text-1" },
          { type: "finish", finishReason: "stop" },
        ]);
      },
    };
    const appendedEvents: SessionEvent[] = [];
    const requestEndEnvelopes: SessionEventWriterRequestEndEnvelope[] = [];
    const baseWriter = writerFrom((envelope) => {
      appendedEvents.push(envelope.event);
      return {
        ok: true,
        writeId: envelope.writeId,
        eventId: `bridge-${envelope.writeId}`,
        processedAt: createdAt,
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
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
            llmService: llm,
            writer,
            providerCallRuntime: {
              systemInstructions: "tool test system",
              toolCatalog: catalogForTest({ name: "search", description: "Search test tool", inputSchema: { type: "object", properties: { q: { type: "string" } } } }),
            },
            runtimePolicy: () => ({ providerRescheduleBudget: 3, compactionRescheduleBudget: 2 }),
          }),
        ),
      ),
    );

    expect(result).toMatchObject({ type: "completed" });
    expect(capturedRequests).toHaveLength(2);
    expect(capturedRequests[0]?.attachments).toEqual([transientAttachment, fileAttachment]);
    expect(capturedRequests[1]?.attachments).toEqual([transientAttachment]);
    expect(capturedRequests[1]?.messages.at(-1)).toMatchObject({
      role: RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_USER,
      status: "completed",
      origin: "runtime",
      parts: [expect.objectContaining({
        text: expect.objectContaining({
          text: expect.stringContaining("deleted-plot.png"),
        }),
      })],
    });
    expect(requestEndEnvelopes).toHaveLength(2);
    expect(requestEndEnvelopes[0]).toMatchObject({
      isError: true,
      errorKind: "provider_error",
      finishReason: "error",
      reschedule: { attempt: 1 },
    });
    expect(requestEndEnvelopes[0]?.consumedAttachmentRefs ?? []).toEqual([]);
    expect(requestEndEnvelopes[0]?.consumedFileAttachments ?? []).toEqual([]);
    expect(requestEndEnvelopes[1]?.consumedAttachmentRefs).toEqual(["att_1"]);
    expect(requestEndEnvelopes[1]?.consumedFileAttachments).toEqual([{
      sourceEventId: "sevt_file_message_1",
      fileId: "file_1",
    }]);
    expect(appendedEvents.filter((event) => event.type === "session.error")).toEqual([
      expect.objectContaining({
        error: expect.objectContaining({ retryStatus: { type: "retrying", attempt: 1 } }),
      }),
    ]);
    const retryErrorIndex = appendedEvents.findIndex((event) => event.type === "session.error");
    const secondRequestStartIndex = appendedEvents.findLastIndex((event) => event.type === "span.model_request_start");
    expect(retryErrorIndex).toBeGreaterThanOrEqual(0);
    expect(retryErrorIndex).toBeLessThan(secondRequestStartIndex);
    expect(appendedEvents.slice(0, secondRequestStartIndex).map((event) => event.type)).not.toContain("session.status_idle");
    expect(session.state.pendingAttachments()).toEqual([]);
    expect(session.state.transientModelMessages()).toEqual([]);
  });

  test("runtime layer commits valid tool errors to hot context after error result ACK", async () => {
    const order: string[] = [];
    const session = new Session("sesn_1");
    const store = new AgentLoopRuntimeStore(order);
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const writer = writerFrom((envelope) => {
      const assistant = session.state.contextManager.messages().find((message) => message.role === "assistant");
      const toolPart = assistant?.parts.find((part) => part.type === "tool");
      order.push(`event:${envelope.event.type}:tool_${toolPart?.type === "tool" ? toolPart.state.status : "missing"}:${envelope.event.type === "agent.tool_result" ? String(envelope.event.is_error) : "progress"}`);
      return { ok: true, writeId: envelope.writeId, eventId: envelope.event.type === "agent.tool_use" ? "bridge-tool" : "bridge-event", processedAt: createdAt };
    });

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
            store,
            writer,
            events: [
              {
                type: "tool-call",
                id: "tool-1",
                toolName: "search",
                input: { q: "x" },
                inputPreview: { value: { q: "x" }, preview: "{\"q\":\"x\"}", truncated: false },
              },
              { type: "finish", finishReason: "tool-calls" },
            ],
            providerCallRuntime: {
              systemInstructions: "tool test system",
              toolCatalog: catalogForTest({ name: "search", description: "Search test tool", inputSchema: { type: "object", properties: { q: { type: "string" } } } }),
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
      "event:span.model_request_end:tool_missing:progress",
      "event:agent.tool_use:tool_missing:progress",
      "event:agent.tool_result:tool_running:true",
      "event:session.status_idle:tool_error:progress",
    ]);
  });

  test("absent cross-family builtins take the durable internal invalid-tool repair path in both directions", async () => {
    for (const tc of [
      { family: "claude" as const, absentTool: "exec_command" },
      { family: "gpt" as const, absentTool: "Bash" },
    ]) {
      const session = new Session("sesn_" + tc.family);
      const order: string[] = [];
      const store = new AgentLoopRuntimeStore(order);
      const publicToolEvents: string[] = [];
      let runToolCalls = 0;
      const loader = new RecordingContextLoader([], {
        type: "messages",
        messages: [userMessage("user-" + tc.family, 0, "hello")],
      });
      const writer = writerFrom((envelope) => {
        if (envelope.event.type === "agent.tool_use" || envelope.event.type === "agent.tool_result") {
          publicToolEvents.push(envelope.event.type);
        }
        return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
      });
      const result = await Effect.runPromise(
        Effect.gen(function* () {
          const agentLoop = yield* AgentLoop.Service;
          return yield* agentLoop.run(session, testRunCustody());
        }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
          store,
          writer,
          llmService: queuedLLMService([
            [
              {
                type: "tool-call",
                id: "tool-other-family",
                toolName: tc.absentTool,
                input: {},
                inputPreview: { value: {}, preview: "{}", truncated: false },
              },
              { type: "finish", finishReason: "tool-calls" },
            ],
            [{ type: "finish", finishReason: "stop" }],
          ]),
          providerCallRuntime: { systemInstructions: "cross-family repair test" },
          runtimePolicy: () => ({ toolCatalog: createToolCatalog({ family: tc.family }) }),
          runTool: () => {
            runToolCalls += 1;
            return { type: "completed", output: { text: "must not run", truncated: false } };
          },
        }))),
      );
      expect(result).toMatchObject({ type: "completed" });
      expect(runToolCalls).toBe(0);
      expect(publicToolEvents).toEqual([]);
      expect(store.repairs).toHaveLength(1);
      expect(store.repairs[0]?.toolName).toBe(tc.absentTool);
      expect(order).toContain("store:internal-tool-repair");
    }
  });

  test("runtime layer schedules same-target tool calls through ToolScheduler", async () => {
    const session = new Session("sesn_1");
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const firstRelease = deferred<void>();
    const calls: string[] = [];
    let active = 0;
    let maxActive = 0;

    const runPromise = Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
            events: [
              {
                type: "tool-call",
                id: "tool-1",
                toolName: "Write",
                input: { file_path: "src/a.ts", content: "one" },
                inputPreview: { value: { file_path: "src/a.ts", content: "one" }, preview: "{\"file_path\":\"src/a.ts\"}", truncated: false },
              },
              {
                type: "tool-call",
                id: "tool-2",
                toolName: "Write",
                input: { file_path: "/workspace/src/a.ts", content: "two" },
                inputPreview: { value: { file_path: "/workspace/src/a.ts", content: "two" }, preview: "{\"file_path\":\"/workspace/src/a.ts\"}", truncated: false },
              },
              { type: "finish", finishReason: "tool-calls" },
            ],
            providerCallRuntime: {
              systemInstructions: "tool scheduler test system",
              toolCatalog: catalogForTest({ name: "Write", description: "Write file", inputSchema: { type: "object" } }),
            },
            runTool: async (request) => {
              calls.push(request.modelToolCallId);
              active += 1;
              maxActive = Math.max(maxActive, active);
              if (request.modelToolCallId === "tool-1") {
                await firstRelease.promise;
              }
              active -= 1;
              return { type: "completed", output: { text: `done ${request.modelToolCallId}`, truncated: false } };
            },
          }),
        ),
      ),
    );

    await waitForCondition(() => calls.length === 1, "first same-target tool start");
    expect(calls).toEqual(["tool-1"]);
    firstRelease.resolve(undefined);
    const result = await runPromise;

    expect(result).toMatchObject({ type: "completed" });
    expect(calls).toEqual(["tool-1", "tool-2"]);
    expect(maxActive).toBe(1);
  });

  test("serializes one shared-message declaration stream while four safe tools execute independently", async () => {
    const session = new Session("sesn_tool_declaration_order");
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const firstDeclarationArrived = deferred<void>();
    const releaseFirstDeclaration = deferred<void>();
    const releaseExecutions = deferred<void>();
    const declarations: SessionEventEnvelope[] = [];
    const settlements: SessionEventEnvelope[] = [];
    const executions: string[] = [];
    let activeExecutions = 0;
    let maxActiveExecutions = 0;
    const baseWriter = writerFrom((envelope) => ({
      ok: true,
      writeId: envelope.writeId,
      eventId: `bridge-${envelope.writeId}`,
      processedAt: createdAt,
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
        } else if (envelope.event.type === "agent.tool_result") {
          settlements.push(envelope);
        }
        return await baseWriter.append(envelope);
      },
    };

    const runPromise = Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        writer,
        events: [
          { type: "reasoning-start", id: "reasoning-1" },
          { type: "reasoning-delta", id: "reasoning-1", text_delta: "first completed reasoning part" },
          { type: "reasoning-end", id: "reasoning-1" },
          { type: "reasoning-start", id: "reasoning-2" },
          { type: "reasoning-delta", id: "reasoning-2", text_delta: "second completed reasoning part" },
          { type: "reasoning-end", id: "reasoning-2" },
          {
            type: "tool-call",
            id: "tool-1",
            toolName: "Read",
            input: { file_path: "src/a.ts" },
            inputPreview: { value: { file_path: "src/a.ts" }, preview: "{\"file_path\":\"src/a.ts\"}", truncated: false },
          },
          {
            type: "tool-call",
            id: "tool-2",
            toolName: "Read",
            input: { file_path: "src/b.ts" },
            inputPreview: { value: { file_path: "src/b.ts" }, preview: "{\"file_path\":\"src/b.ts\"}", truncated: false },
          },
          {
            type: "tool-call",
            id: "tool-3",
            toolName: "Read",
            input: { file_path: "src/c.ts", query: "x".repeat(9_000) },
            inputPreview: { preview: "x".repeat(8_192), truncated: true },
          },
          {
            type: "tool-call",
            id: "tool-4",
            toolName: "Read",
            input: { file_path: "src/d.ts" },
            inputPreview: { value: { file_path: "src/d.ts" }, preview: "{\"file_path\":\"src/d.ts\"}", truncated: false },
          },
          { type: "finish", finishReason: "tool-calls" },
        ],
        providerCallRuntime: {
          systemInstructions: "tool declaration ordering test system",
          toolCatalog: catalogForTest({ name: "Read", description: "Read file", inputSchema: { type: "object" } }),
        },
        runTool: async (request) => {
          executions.push(request.modelToolCallId);
          activeExecutions += 1;
          maxActiveExecutions = Math.max(maxActiveExecutions, activeExecutions);
          await releaseExecutions.promise;
          activeExecutions -= 1;
          return { type: "completed", output: { text: `done ${request.modelToolCallId}`, truncated: false } };
        },
      }))),
    );

    await firstDeclarationArrived.promise;
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(declarations).toHaveLength(1);
    releaseFirstDeclaration.resolve(undefined);
    await waitForCondition(() => executions.length === 4, "four parallel safe tool executions");
    expect(executions).toEqual(["tool-1", "tool-2", "tool-3", "tool-4"]);
    expect(maxActiveExecutions).toBe(4);
    releaseExecutions.resolve(undefined);

    expect(await runPromise).toMatchObject({ type: "completed" });
    expect(settlements).toHaveLength(4);
    const toolCallIds = declarations.map((envelope) =>
      envelope.drafts[0]?.parts.filter((part) => part.type === "tool").map((part) => part.toolCallId)
    );
    expect(toolCallIds).toEqual([
      ["tool-1"],
      ["tool-1", "tool-2"],
      ["tool-1", "tool-2", "tool-3"],
      ["tool-1", "tool-2", "tool-3", "tool-4"],
    ]);
  });

  test("holds an earlier Tool Result behind a sibling Tool Use declaration ACK", async () => {
    const session = new Session("sesn_tool_projection_order");
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const secondDeclarationArrived = deferred<void>();
    const releaseSecondDeclaration = deferred<void>();
    const releaseFirstExecution = deferred<void>();
    const declarations: SessionEventEnvelope[] = [];
    const settlements: SessionEventEnvelope[] = [];
    const baseWriter = writerFrom((envelope) => ({
      ok: true,
      writeId: envelope.writeId,
      eventId: `bridge-${envelope.writeId}`,
      processedAt: createdAt,
    }));
    const writer: SessionEventWriter = {
      ...baseWriter,
      append: async (envelope) => {
        if (envelope.event.type === "agent.tool_use") {
          declarations.push(envelope);
          if (declarations.length === 2) {
            secondDeclarationArrived.resolve(undefined);
            await releaseSecondDeclaration.promise;
          }
        } else if (envelope.event.type === "agent.tool_result") {
          settlements.push(envelope);
        }
        return await baseWriter.append(envelope);
      },
    };

    const runPromise = Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        writer,
        events: [
          {
            type: "tool-call",
            id: "tool-1",
            toolName: "Read",
            input: { file_path: "src/a.ts" },
            inputPreview: { value: { file_path: "src/a.ts" }, preview: "{\"file_path\":\"src/a.ts\"}", truncated: false },
          },
          {
            type: "tool-call",
            id: "tool-2",
            toolName: "Read",
            input: { file_path: "src/b.ts" },
            inputPreview: { value: { file_path: "src/b.ts" }, preview: "{\"file_path\":\"src/b.ts\"}", truncated: false },
          },
          { type: "finish", finishReason: "tool-calls" },
        ],
        providerCallRuntime: {
          systemInstructions: "tool projection ordering test system",
          toolCatalog: catalogForTest({ name: "Read", description: "Read file", inputSchema: { type: "object" } }),
        },
        runTool: async (request) => {
          if (request.modelToolCallId === "tool-1") {
            await releaseFirstExecution.promise;
          }
          return { type: "completed", output: { text: `done ${request.modelToolCallId}`, truncated: false } };
        },
      }))),
    );

    await secondDeclarationArrived.promise;
    releaseFirstExecution.resolve(undefined);
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(settlements).toHaveLength(0);
    releaseSecondDeclaration.resolve(undefined);

    expect(await runPromise).toMatchObject({ type: "completed" });
    expect(settlements).toHaveLength(2);
  });

  test("separate thread RequestTurns share session-wide tool admission", async () => {
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
    const firstSession = new Session(identity("thrd_a"), undefined, coordinator);
    const secondSession = new Session(identity("thrd_b"), undefined, coordinator);
    const releaseFirst = deferred<void>();
    const starts: string[] = [];
    const events: readonly LLMEvent[] = [
      {
        type: "tool-call",
        id: "tool-memory",
        toolName: "memory",
        input: { action: "view", path: "notes" },
        inputPreview: { value: { action: "view", path: "notes" }, preview: "{\"action\":\"view\",\"path\":\"notes\"}", truncated: false },
      },
      { type: "finish", finishReason: "tool-calls" },
    ];
    const makeLayer = (threadId: string) => runtimeAgentLoopLayer(
      new RecordingContextLoader([], { type: "messages", messages: [userMessage(`user-${threadId}`, 0, "inspect memory")] }),
      {
        events,
        approvalMode: "full_access",
        providerCallRuntime: {
          systemInstructions: "session tool admission test system",
          toolCatalog: catalogForTest({ name: "memory", description: "Memory", inputSchema: { type: "object" } }),
        },
        runTool: async (request) => {
          starts.push(request.sessionThreadId);
          if (request.sessionThreadId === "thrd_a") {
            await releaseFirst.promise;
          }
          return { type: "completed", output: { text: "done", truncated: false } };
        },
      },
    );
    const run = (session: Session, layer: Layer.Layer<AgentLoop.Service>) => Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(layer)),
    );

    const first = run(firstSession, makeLayer("thrd_a"));
    await waitForCondition(() => starts.length === 1, "first session-wide tool start");
    const second = run(secondSession, makeLayer("thrd_b"));
    await new Promise((resolve) => setTimeout(resolve, 5));
    expect(starts).toEqual(["thrd_a"]);

    releaseFirst.resolve(undefined);
    await Promise.all([first, second]);
    expect(starts).toEqual(["thrd_a", "thrd_b"]);
  });

  test("Memory projection replay stays in one ToolFiber until one final settlement", async () => {
    const session = new Session("sesn_1");
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "remember this")] });
    const firstProjectionFailure = deferred<void>();
    const releaseProjectionSuccess = deferred<void>();
    const appended: SessionEvent[] = [];
    let toolRunnerCalls = 0;
    let bridgeAttempts = 0;
    let runSettlements = 0;
    const writer = writerFrom((envelope) => {
      appended.push(envelope.event);
      return {
        ok: true,
        writeId: envelope.writeId,
        eventId: envelope.event.type === "agent.tool_use" ? "sevt_memory_projection" : `bridge-${envelope.writeId}`,
        processedAt: createdAt,
      };
    });

    const runPromise = Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        writer,
        events: [
          {
            type: "tool-call",
            id: "tool-memory",
            toolName: "memory",
            input: { action: "create", path: "notes/todo.md", content: "one" },
            inputPreview: { value: { action: "create", path: "notes/todo.md", content: "one" }, preview: "{\"action\":\"create\"}", truncated: false },
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
          return { type: "completed", output: { text: "memory stored", truncated: false } };
        },
      }))),
    ).then((result) => {
      runSettlements++;
      return result;
    });

    await firstProjectionFailure.promise;
    expect(runSettlements).toBe(0);
    expect(appended.filter((event) => event.type === "agent.tool_result")).toHaveLength(0);
    expect(appended.filter((event) => event.type === "session.status_idle")).toHaveLength(0);
    expect(JSON.stringify(appended)).not.toContain("projection_refresh_failed");
    const pendingToolPart = session.state.contextManager.messages().at(-1)?.parts.find((part) => part.type === "tool");
    expect(pendingToolPart?.type === "tool" ? pendingToolPart.state.status : undefined).toBe("running");
    releaseProjectionSuccess.resolve(undefined);
    const result = await runPromise;

    expect(result).toMatchObject({ type: "completed" });
    expect(toolRunnerCalls).toBe(1);
    expect(bridgeAttempts).toBe(2);
    expect(runSettlements).toBe(1);
    expect(appended.filter((event) => event.type === "agent.tool_result")).toEqual([
      expect.objectContaining({ tool_use_id: "sevt_memory_projection" }),
    ]);
    expect(appended.filter((event) => event.type === "session.status_idle")).toHaveLength(1);
    const completedToolPart = session.state.contextManager.messages().at(-1)?.parts.find((part) => part.type === "tool");
    expect(completedToolPart?.type === "tool" ? completedToolPart.state.status : undefined).toBe("completed");
  });

  test("user interrupt after normal request assembly starts no assistant shell, span, or provider", async () => {
    const session = new Session("sesn_1");
    const storeOrder: string[] = [];
    const store = new AgentLoopRuntimeStore(storeOrder);
    const loader = new RecordingContextLoader([], {
      type: "messages",
      messages: [userMessage("user-1", 0, "do not start")],
    });
    const appended: SessionEvent[] = [];
    let providerCalls = 0;
    let interruptCommits = 0;

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        store,
        writer: writerFrom((envelope) => {
          appended.push(envelope.event);
          return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
        }),
        providerCallAssembler: async (input) => {
          beginTestUserInterrupt(session, "rin_before_shell", () => interruptCommits++);
          return await assembleProviderCallRequest(input);
        },
        llmService: {
          stream() {
            providerCalls++;
            return Stream.empty;
          },
        },
      }))),
    );

    expect(result).toEqual({ type: "interrupted" });
    expect(interruptCommits).toBe(1);
    expect(providerCalls).toBe(0);
    expect(storeOrder).toEqual([]);
    expect(appended.filter((event) => event.type === "span.model_request_start")).toEqual([]);
    expect(appended.filter((event) => event.type === "span.model_request_end")).toEqual([]);
    expect(appended.at(-1)).toEqual({ type: "session.status_idle", stop_reason: { type: "end_turn" } });
  });

  test("stale pre-provider interrupt commit discards hot state without FinishIdle", async () => {
    const session = new Session("sesn_stale_pre_provider_interrupt");
    const loader = new RecordingContextLoader([], {
      type: "messages",
      messages: [userMessage("user-stale-pre-provider", 0, "do not start")],
    });
    let finishIdleCalls = 0;
    let providerCalls = 0;
    const baseWriter = writerFrom((envelope) => ({
      ok: true,
      writeId: envelope.writeId,
      eventId: `bridge-${envelope.writeId}`,
      processedAt: createdAt,
    }));
    const writer: SessionEventWriter = {
      ...baseWriter,
      finishIdle: async (envelope) => {
        finishIdleCalls += 1;
        return await baseWriter.finishIdle!(envelope);
      },
    };

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        writer,
        providerCallAssembler: async (input) => {
          const command = acceptedInput("rin_stale_pre_provider_interrupt");
          session.state.beginUserInterrupt(command, async () => ({ ok: true, stale: true }));
          return await assembleProviderCallRequest(input);
        },
        llmService: {
          stream() {
            providerCalls += 1;
            return Stream.empty;
          },
        },
      }))),
    );

    expect(result).toEqual({ type: "interrupted", discardHotState: true });
    expect(providerCalls).toBe(0);
    expect(finishIdleCalls).toBe(0);
  });

  test("user interrupt after normal span ACK closes it before any provider call", async () => {
    const session = new Session("sesn_1");
    const attachment = {
      transient: {
        attachmentRef: "att_interrupt_before_provider",
        sourceToolUseEventId: "sevt_tool_interrupt_before_provider",
        sourcePath: "mcp:github/interrupt.png",
        pageRange: "",
        detail: "auto",
      },
      fileBacked: undefined,
      mime: "image/png",
      filename: "interrupt.png",
    } as const;
    session.state.addPendingAttachments([attachment]);
    const store = new AgentLoopRuntimeStore([]);
    const loader = new RecordingContextLoader([], {
      type: "messages",
      messages: [userMessage("user-1", 0, "stop before provider")],
    });
    const appended: SessionEvent[] = [];
    const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
    let providerCalls = 0;
    let interrupted = false;
    const baseWriter = writerFrom((envelope) => {
      appended.push(envelope.event);
      if (!interrupted && envelope.event.type === "span.model_request_start") {
        interrupted = true;
        beginTestUserInterrupt(session, "rin_after_span");
      }
      return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
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
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        store,
        writer,
        llmService: {
          stream() {
            providerCalls++;
            return Stream.empty;
          },
        },
      }))),
    );

    expect(result).toEqual({ type: "interrupted" });
    expect(providerCalls).toBe(0);
    expect(appended.filter((event) => event.type === "span.model_request_start")).toHaveLength(1);
    expect(requestEnds).toEqual([
      expect.objectContaining({ isError: true, errorKind: "runtime_interrupted", finishReason: "cancelled" }),
    ]);
    expect(requestEnds[0]?.consumedAttachmentRefs ?? []).toEqual([]);
    expect(session.state.pendingAttachments()).toEqual([attachment]);
  });

  test("provider interruption exposes request-end rejection through the Effect Cause", async () => {
    const session = new Session("sesn_provider_closeout_rejected");
    const loader = new RecordingContextLoader([], {
      type: "messages",
      messages: [userMessage("user-provider-closeout", 0, "stop before provider")],
    });
    let interrupted = false;
    const baseWriter = writerFrom((envelope) => {
      if (!interrupted && envelope.event.type === "span.model_request_start") {
        interrupted = true;
        beginTestUserInterrupt(session, "rin_provider_closeout_rejected");
      }
      return {
        ok: true,
        writeId: envelope.writeId,
        eventId: `bridge-${envelope.writeId}`,
        processedAt: createdAt,
      };
    });
    const writer: SessionEventWriter = {
      ...baseWriter,
      writeRequestEnd: async (envelope) => ({
        ok: false,
        error: {
          type: "session-event-writer",
          code: "unavailable",
          message: "request end not ACKed",
          retryable: false,
          fatal: false,
          sessionId: envelope.sessionId,
          writeId: envelope.writeId,
        },
      }),
    };

    const runExit = await Effect.runPromiseExit(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        writer,
        llmService: {
          stream() {
            throw new Error("provider must not start");
          },
        },
      }))),
    );

    expect(Exit.isFailure(runExit)).toBe(true);
    if (Exit.isFailure(runExit)) {
      expect(runExit.cause.reasons.find(Cause.isDieReason)?.defect).toMatchObject({
        type: "session-event-writer",
        code: "unavailable",
      });
    }
    expect(session.state.userInterruptCommitResult("rin_provider_closeout_rejected")).toEqual({
      ok: false,
      retryable: false,
      errorCode: "unavailable",
    });
  });

  test("a stale interrupt request-end receipt performs no fallback idle closeout", async () => {
    const session = new Session("sesn_stale_interrupt_end");
    const loader = new RecordingContextLoader([], {
      type: "messages",
      messages: [userMessage("user-stale-interrupt", 0, "stop before provider")],
    });
    let interrupted = false;
    let requestEndCalls = 0;
    let finishIdleCalls = 0;
    const baseWriter = writerFrom((envelope) => {
      if (!interrupted && envelope.event.type === "span.model_request_start") {
        interrupted = true;
        beginTestUserInterrupt(session, "rin_stale_interrupt_end");
      }
      return {
        ok: true,
        writeId: envelope.writeId,
        eventId: `bridge-${envelope.writeId}`,
        processedAt: createdAt,
      };
    });
    const writer: SessionEventWriter = {
      ...baseWriter,
      writeRequestEnd: async (envelope) => {
        requestEndCalls += 1;
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
      finishIdle: async (envelope) => {
        finishIdleCalls += 1;
        return await baseWriter.finishIdle!(envelope);
      },
    };

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        writer,
        llmService: {
          stream() {
            throw new Error("provider must not start");
          },
        },
      }))),
    );

    expect(result).toEqual({ type: "interrupted", discardHotState: true });
    expect(requestEndCalls).toBe(1);
    expect(finishIdleCalls).toBe(0);
    expect(session.state.userInterruptCommitResult("rin_stale_interrupt_end")).toEqual({
      ok: true,
      stale: true,
    });
  });

  test("cooperative child cancellation closes an ACKed request before run release", async () => {
    const session = new Session("sesn_1");
    const attachment = {
      transient: {
        attachmentRef: "att_cooperative_before_provider",
        sourceToolUseEventId: "sevt_tool_cooperative_before_provider",
        sourcePath: "mcp:github/cooperative.png",
        pageRange: "",
        detail: "auto",
      },
      fileBacked: undefined,
      mime: "image/png",
      filename: "cooperative.png",
    } as const;
    session.state.addPendingAttachments([attachment]);
    const loader = new RecordingContextLoader([], {
      type: "messages",
      messages: [userMessage("user-1", 0, "review this")],
    });
    const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
    let providerCalls = 0;
    const writer = writerFrom(
      (envelope) => {
        if (envelope.event.type === "span.model_request_start") {
          session.state.beginCooperativeCancel();
        }
        return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
      },
      async (envelope) => {
        requestEnds.push(envelope);
        return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
      },
    );
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        writer,
        llmService: {
          stream() {
            providerCalls++;
            return Stream.empty;
          },
        },
      }))),
    );

    expect(result).toEqual({ type: "interrupted" });
    expect(providerCalls).toBe(0);
    expect(requestEnds).toEqual([
      expect.objectContaining({ isError: true, errorKind: "runtime_interrupted", finishReason: "cancelled" }),
    ]);
    expect(requestEnds[0]?.consumedAttachmentRefs ?? []).toEqual([]);
    expect(session.state.pendingAttachments()).toEqual([attachment]);
  });

  test("cooperative cancellation before request start is write-free", async () => {
    const session = new Session("sesn_1");
    session.state.beginCooperativeCancel();
    const appended: SessionEvent[] = [];
    const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
    const baseWriter = writerFrom((envelope) => {
      appended.push(envelope.event);
      return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
    });
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(
        new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "review")] }),
        {
          writer: { ...baseWriter, writeRequestEnd: async (envelope) => {
            requestEnds.push(envelope);
            return { ok: true, writeId: envelope.writeId, eventId: envelope.writeId, processedAt: createdAt };
          } },
        },
      ))),
    );
    expect(result).toEqual({ type: "interrupted" });
    expect(appended).toEqual([]);
    expect(requestEnds).toEqual([]);
  });

  test("user interrupt after compaction assembly starts no compaction span or provider", async () => {
    const session = new Session("sesn_1");
    recordCompactionHint(session, {
      inputTokens: 200,
      outputTokens: 75,
      reasoningTokens: 0,
      cacheReadTokens: 0,
      cacheWriteTokens: 0,
    });
    const loader = new RecordingContextLoader([], {
      type: "messages",
      messages: [userMessage("user-1", 0, compactionHistory("compact but stop"))],
    });
    const appended: SessionEvent[] = [];
    let providerCalls = 0;

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        compaction: {},
        writer: writerFrom((envelope) => {
          appended.push(envelope.event);
          return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
        }),
        providerCallAssembler: async (input) => {
          if (input.runtime.requestKind === ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY) {
            beginTestUserInterrupt(session, "rin_before_compaction_span");
          }
          return await assembleProviderCallRequest(input);
        },
        llmService: {
          stream() {
            providerCalls++;
            return Stream.empty;
          },
        },
      }))),
    );

    expect(result).toEqual({ type: "interrupted" });
    expect(providerCalls).toBe(0);
    expect(appended.filter((event) => event.type === "span.model_request_start")).toEqual([]);
    expect(appended.filter((event) => event.type === "span.model_request_end")).toEqual([]);
  });

  test("user interrupt after compaction span ACK closes it before any provider call", async () => {
    const session = new Session("sesn_1");
    recordCompactionHint(session, {
      inputTokens: 200,
      outputTokens: 75,
      reasoningTokens: 0,
      cacheReadTokens: 0,
      cacheWriteTokens: 0,
    });
    const loader = new RecordingContextLoader([], {
      type: "messages",
      messages: [userMessage("user-1", 0, compactionHistory("compact then stop"))],
    });
    const appended: SessionEvent[] = [];
    const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
    let providerCalls = 0;
    let interrupted = false;
    const writer = writerFrom((envelope) => {
      appended.push(envelope.event);
      if (!interrupted && envelope.event.type === "span.model_request_start") {
        interrupted = true;
        beginTestUserInterrupt(session, "rin_after_compaction_span");
      }
      return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
    }, async (envelope) => {
      requestEnds.push(envelope);
      return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
    });

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        compaction: {},
        writer,
        llmService: {
          stream() {
            providerCalls++;
            return Stream.empty;
          },
        },
      }))),
    );

    expect(result).toEqual({ type: "interrupted" });
    expect(providerCalls).toBe(0);
    expect(appended.filter((event) => event.type === "span.model_request_start")).toHaveLength(1);
    expect(requestEnds).toEqual([
      expect.objectContaining({
        requestKind: "compaction_summary",
        isError: true,
        errorKind: "runtime_interrupted",
        finishReason: "cancelled",
      }),
    ]);
  });

  test("interrupt joins a pre-fence agent.tool_use Bridge ACK beyond the route bound before repair and snapshot", async () => {
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
        writeId: envelope.writeId,
        eventId: envelope.event.type === "agent.tool_use" ? "sevt_gated_tool_use" : `bridge-${envelope.writeId}`,
        processedAt: createdAt,
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
    const agentLayer = runtimeAgentLoopLayer(loader, {
      writer,
      llmService: {
        stream(_request, options) {
          return Stream.fromAsyncIterable((async function* () {
            yield {
              type: "tool-call" as const,
              id: "tool-gated",
              toolName: "Write",
              input: { file_path: "src/gated.ts", content: "one" },
              inputPreview: { value: { file_path: "src/gated.ts", content: "one" }, preview: "{}", truncated: false },
            };
            if (options?.abortSignal === undefined) {
              throw new Error("provider stream requires an abort signal");
            }
            await waitForReleaseOrAbort(providerRelease.promise, options.abortSignal);
            yield { type: "finish" as const, finishReason: "tool-calls" as const };
          })(), (error): LLMServiceError => ({
            type: "llm-service",
            error: runtimeFailureFromProviderError(normalizeProviderError({
              code: "provider_stream_error",
              message: String(error),
              retryable: true,
            })),
          }));
        },
      },
      providerCallRuntime: {
        systemInstructions: "gated tool-use append test",
        toolCatalog: catalogForTest({ name: "Write", description: "Write file", inputSchema: { type: "object" } }),
      },
      runTool: () => {
        toolRunnerCalls++;
        return { type: "completed", output: { text: "must not run", truncated: false } };
      },
    });
    const managerLayer = SessionManager.layer({ maxLocalSessions: 4, now: () => createdAt }).pipe(Layer.provide(agentLayer));
    const { manager, scope } = await Effect.runPromise(Effect.gen(function* () {
      const layerScope = yield* Scope.make();
      const context = yield* Layer.buildWithScope(managerLayer, layerScope);
      return { manager: Context.get(context, SessionManager.Service), scope: layerScope };
    }));

    try {
      const input = acceptedInput("rin_gated_tool_use");
      await Effect.runPromise(manager.preloadThread({
        ...input,
        runtimeBindingToken: "runtime-binding-token",
        coldCoverage: emptyColdCoverage,
        messages: [userMessage("user-1", 0, "run the gated tool")],
        thread: { role: "main", visibility: "public", agentType: "general", status: "idle" },
      }));
      await Effect.runPromise(manager.acceptInput(input));
      await toolUseAppendStarted.promise;
      const command = { ...acceptedInput("rin_gated_tool_use_interrupt"), sequenceFrom: 9, sequenceTo: 9 };
      const interrupt = Effect.runPromise(manager.interruptControl("sesn_1", command, async (declaration) => {
        interruptCommitStarted = true;
        order.push("commit:interrupt");
        return buildRuntimeControlCommitResult(command, "interrupt_control", declaration);
      }));
      await new Promise<void>((resolve) => setImmediate(resolve));
      jest.useFakeTimers();
      await flushMicrotasks();
      jest.advanceTimersByTime(350);
      await flushMicrotasks();
      expect(interruptCommitStarted).toBe(false);
      expect(toolRunnerCalls).toBe(0);

      releaseToolUseAppend.resolve();
      for (let attempt = 0; attempt < 5; attempt++) {
        jest.advanceTimersByTime(1_000);
        await flushMicrotasks();
      }
      expect(await interrupt).toMatchObject({ ok: true, interrupted: true });

      expect(toolRunnerCalls).toBe(0);
      expect(appended.filter((event) => event.type === "agent.tool_use")).toHaveLength(1);
      expect(appended.filter((event) => event.type === "agent.tool_result")).toEqual([]);
      expect(requestEnds).toHaveLength(1);
      const cancellationDraft = requestEnds[0]?.drafts?.find((draft) => draft.draftKind === "cancellation");
      expect(cancellationDraft).toMatchObject({
        sourceKind: "interrupt_control",
        sourceId: command.runtimeInputId,
        sourceEventId: command.eventIds[0],
        parts: [expect.objectContaining({
          type: "tool",
          toolUseEventId: "sevt_gated_tool_use",
          state: expect.objectContaining({ status: "cancelled" }),
        })],
      });
      if (cancellationDraft === undefined) {
        throw new Error("missing joined interrupt cancellation draft");
      }
      expect(requestEnds[0]?.interruptSettlement).toEqual({
        runtimeInputId: command.runtimeInputId,
        eventIds: [...command.eventIds],
        sequenceFrom: command.sequenceFrom,
        sequenceTo: command.sequenceTo,
        pendingToolCancellations: [],
        sandboxExecutionToolUseEventIds: [],
      });
      expect(await Effect.runPromise(manager.inspectThread(command))).toMatchObject({
        ok: true,
        observed: true,
        hasPendingApprovalToolJobs: false,
      });
      expect(interruptCommitStarted).toBe(false);
      expect(order.indexOf("tool-use-append:ack")).toBeLessThan(order.indexOf("event:span.model_request_end"));
      expect(order.indexOf("event:span.model_request_end")).toBeLessThan(order.indexOf("event:session.status_idle"));
    } finally {
      jest.useRealTimers();
      releaseToolUseAppend.resolve();
      providerRelease.resolve();
      await Effect.runPromise(Scope.close(scope, Exit.void));
    }
  });

  test("interrupt joins a raw CommitInternalToolRepair ACK before snapshot and permits no late durable repair", async () => {
    const loader = new QueuedContextLoader([], []);
    const repairStarted = deferred<void>();
    const releaseRepair = deferred<void>();
    const releaseRepairWrapperTimeout = deferred<boolean>();
    const providerRelease = deferred<void>();
    const order: string[] = [];
    let interruptCommitStarted = false;
    const store = new AgentLoopRuntimeStore(order, false, false, undefined, async () => {
      order.push("repair:start");
      repairStarted.resolve();
      await releaseRepair.promise;
      order.push("repair:ack");
    });
    const appended: SessionEvent[] = [];
    let storeSleepCalls = 0;
    const agentLayer = runtimeAgentLoopLayer(loader, {
      store,
      runtime: {
        ...agentLoopRuntime(),
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
            signal.addEventListener("abort", () => resolve(false), { once: true });
          });
        },
      },
      writer: writerFrom((envelope) => {
        appended.push(envelope.event);
        order.push(`event:${envelope.event.type}`);
        return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
      }),
      llmService: {
        stream(_request, options) {
          return Stream.fromAsyncIterable((async function* () {
            yield {
              type: "tool-call" as const,
              id: "tool-invalid",
              toolName: "MissingTool",
              input: {},
              inputPreview: { value: {}, preview: "{}", truncated: false },
            };
            if (options?.abortSignal === undefined) {
              throw new Error("provider stream requires an abort signal");
            }
            await waitForReleaseOrAbort(providerRelease.promise, options.abortSignal);
            yield { type: "finish" as const, finishReason: "tool-calls" as const };
          })(), (error): LLMServiceError => ({
            type: "llm-service",
            error: runtimeFailureFromProviderError(normalizeProviderError({
              code: "provider_stream_error",
              message: String(error),
              retryable: true,
            })),
          }));
        },
      },
      providerCallRuntime: {
        systemInstructions: "gated internal repair test",
        toolCatalog: catalogForTest({ name: "Write", description: "Write file", inputSchema: { type: "object" } }),
      },
    });
    const managerLayer = SessionManager.layer({ maxLocalSessions: 4, now: () => createdAt }).pipe(Layer.provide(agentLayer));
    const { manager, scope } = await Effect.runPromise(Effect.gen(function* () {
      const layerScope = yield* Scope.make();
      const context = yield* Layer.buildWithScope(managerLayer, layerScope);
      return { manager: Context.get(context, SessionManager.Service), scope: layerScope };
    }));

    try {
      const input = acceptedInput("rin_gated_internal_repair");
      await Effect.runPromise(manager.preloadThread({
        ...input,
        runtimeBindingToken: "runtime-binding-token",
        coldCoverage: emptyColdCoverage,
        messages: [userMessage("user-1", 0, "trigger an internal repair")],
        thread: { role: "main", visibility: "public", agentType: "general", status: "idle" },
      }));
      await Effect.runPromise(manager.acceptInput(input));
      await repairStarted.promise;
      const command = { ...acceptedInput("rin_gated_internal_repair_interrupt"), sequenceFrom: 9, sequenceTo: 9 };
      const interrupt = Effect.runPromise(manager.interruptControl("sesn_1", command, async (declaration) => {
        interruptCommitStarted = true;
        order.push("commit:interrupt");
        return buildRuntimeControlCommitResult(command, "interrupt_control", declaration);
      }));
      await flushMicrotasks();
      expect(interruptCommitStarted).toBe(false);
      releaseRepairWrapperTimeout.resolve(true);
      await flushMicrotasks();
      expect(interruptCommitStarted).toBe(false);

      releaseRepair.resolve();
      expect({ interruptResult: await interrupt, order }).toMatchObject({
        interruptResult: { ok: true, interrupted: true },
      });
      const operationCountAtCloseout = order.filter((entry) => entry === "store:internal-tool-repair").length;
      await flushMicrotasks();

      expect(operationCountAtCloseout).toBe(1);
      expect(order.filter((entry) => entry === "store:internal-tool-repair")).toHaveLength(1);
      expect(interruptCommitStarted).toBe(false);
      expect(order.indexOf("repair:ack")).toBeLessThan(order.indexOf("event:span.model_request_end"));
      expect(order.indexOf("event:span.model_request_end")).toBeLessThan(order.indexOf("event:session.status_idle"));
      expect(appended.filter((event) => event.type === "session.error")).toEqual([]);
    } finally {
      releaseRepair.resolve();
      releaseRepairWrapperTimeout.resolve(false);
      providerRelease.resolve();
      await Effect.runPromise(Scope.close(scope, Exit.void));
    }
  });

  test("failed interrupt request-end leaves the snapshot and FinishIdle unwritten for Bridge repair", async () => {
    const session = new Session("sesn_1");
    const loader = new RecordingContextLoader([], {
      type: "messages",
      messages: [userMessage("user-request-end-failure", 0, "interrupt after span start")],
    });
    const appended: SessionEvent[] = [];
    let interruptCommits = 0;
    let finishIdleCalls = 0;
    const baseWriter = writerFrom((envelope) => {
      appended.push(envelope.event);
      if (envelope.event.type === "span.model_request_start") {
        beginTestUserInterrupt(session, "rin_request_end_failure", () => {
          interruptCommits++;
        });
      }
      return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
    });
    const writer: SessionEventWriter = {
      ...baseWriter,
      writeRequestEnd: async (envelope) => ({
        ok: false,
        error: {
          type: "session-event-writer",
          code: "unavailable",
          message: "request end unavailable",
          retryable: false,
          fatal: true,
          sessionId: envelope.sessionId,
          writeId: envelope.writeId,
        },
      }),
      finishIdle: async (envelope) => {
        finishIdleCalls++;
        return withFinishIdleReceiptForTest(envelope, {
          ok: true,
          writeId: envelope.durableTurnId,
          eventId: `bridge-${envelope.durableTurnId}`,
          processedAt: createdAt,
        });
      },
    };

    const runExit = await Effect.runPromiseExit(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        writer,
        llmService: {
          stream() {
            throw new Error("provider must not start after the interrupt wins at span ACK");
          },
        },
      }))),
    );

    expect(Exit.isFailure(runExit)).toBe(true);
    if (Exit.isFailure(runExit)) {
      expect(runExit.cause.reasons.find(Cause.isDieReason)?.defect).toMatchObject({
        type: "session-event-writer",
        code: "unavailable",
      });
    }
    expect(interruptCommits).toBe(0);
    expect(finishIdleCalls).toBe(0);
    expect(appended.filter((event) => event.type === "session.status_idle")).toEqual([]);
  });

  test("failed interrupt receipt application leaves FinishIdle unwritten and surfaces through the Effect Cause", async () => {
    const session = new Session("sesn_1");
    const loader = new RecordingContextLoader([], {
      type: "messages",
      messages: [userMessage("user-repair-failure", 0, "run then interrupt")],
    });
    const appended: SessionEvent[] = [];
    const toolUseWritten = deferred<void>();
    let interruptCommits = 0;
    let finishIdleCalls = 0;
    const baseWriter = writerFrom((envelope) => {
      appended.push(envelope.event);
      if (envelope.event.type === "agent.tool_use") {
        beginTestUserInterrupt(session, "rin_repair_failure", () => {
          interruptCommits++;
        });
        toolUseWritten.resolve();
      }
      return {
        ok: true,
        writeId: envelope.writeId,
        eventId: envelope.event.type === "agent.tool_use" ? "sevt_repair_failure" : `bridge-${envelope.writeId}`,
        processedAt: createdAt,
      };
    });
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
            relatedReceipts: result.declaration.relatedReceipts?.map((receipt) =>
              receipt.sourceKind === "interrupt_control"
                ? { ...receipt, messages: [] }
                : receipt
            ),
          },
        };
      },
      finishIdle: async (envelope) => {
        finishIdleCalls++;
        return withFinishIdleReceiptForTest(envelope, {
          ok: true,
          writeId: envelope.durableTurnId,
          eventId: `bridge-${envelope.durableTurnId}`,
          processedAt: createdAt,
        });
      },
    };
    const runFiber = Effect.runFork(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        writer,
        providerCallRuntime: {
          systemInstructions: "interrupt repair failure test",
          toolCatalog: catalogForTest({ name: "Write", description: "Write file", inputSchema: { type: "object" } }),
        },
        llmService: {
          stream(_request, streamOptions) {
            return Stream.fromAsyncIterable((async function* () {
              yield {
                type: "tool-call" as const,
                id: "tool-repair-failure",
                toolName: "Write",
                input: { file_path: "src/failure.ts", content: "one" },
                inputPreview: { value: { file_path: "src/failure.ts", content: "one" }, preview: "{}", truncated: false },
              };
              if (streamOptions?.abortSignal === undefined) {
                throw new Error("provider stream requires an abort signal");
              }
              await waitForReleaseOrAbort(new Promise<void>(() => undefined), streamOptions.abortSignal);
            })(), (error): LLMServiceError => ({
              type: "llm-service",
              error: runtimeFailureFromProviderError(normalizeProviderError({
                code: "provider_stream_error",
                message: String(error),
                retryable: true,
              })),
            }));
          },
        },
      }))),
    );

    await toolUseWritten.promise;
    await Effect.runPromise(Fiber.interrupt(runFiber));
    const runExit = await Effect.runPromise(Fiber.await(runFiber));

    expect(Exit.isFailure(runExit)).toBe(true);
    if (Exit.isFailure(runExit)) {
      expect(runExit.cause.reasons.find(Cause.isDieReason)?.defect).toMatchObject({
        type: "runtime",
        code: "runtime_invalid_sequence",
        reason: "runtime_contract_validation",
      });
    }
    expect(interruptCommits).toBe(0);
    expect(finishIdleCalls).toBe(0);
    expect(appended.filter((event) => event.type === "session.status_idle")).toEqual([]);
  });

  test("post-success cooperative repair failure settles the attachment ride already consumed by Bridge", async () => {
    const session = new Session("sesn_1");
    const attachment = {
      transient: {
        attachmentRef: "att_post_success_cooperative_failure",
        sourceToolUseEventId: "sevt_post_success_cooperative_failure",
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
      type: "messages",
      messages: [userMessage("user-post-success-cooperative-failure", 0, "run the tool")],
    });
    const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
    let interruptOwner: (() => void) | undefined;
    const baseWriter = writerFrom((envelope) => {
      if (failRepairWrite && envelope.event.type === "agent.tool_result") {
        repairAttempted = true;
        return {
          ok: false,
          error: normalizeSessionEventWriterError({
            code: "unavailable",
            sessionId: envelope.sessionId,
            writeId: envelope.writeId,
          }),
        };
      }
      return {
        ok: true,
        writeId: envelope.writeId,
        eventId: envelope.event.type === "agent.tool_use"
          ? "sevt_post_success_cooperative_failure"
          : `bridge-${envelope.writeId}`,
        processedAt: createdAt,
      };
    });
    const writer: SessionEventWriter = {
      ...baseWriter,
      writeRequestEnd: async (envelope) => {
        requestEnds.push(envelope);
        return await baseWriter.writeRequestEnd(envelope);
      },
    };

    const runFiber = Effect.runFork(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        writer,
        providerCallRuntime: {
          systemInstructions: "post-success cooperative repair failure test",
          toolCatalog: catalogForTest({ name: "Write", description: "Write file", inputSchema: { type: "object" } }),
        },
        events: [
          {
            type: "tool-call",
            id: "tool-post-success-cooperative-failure",
            toolName: "Write",
            input: { file_path: "src/failure.ts", content: "one" },
            inputPreview: { value: { file_path: "src/failure.ts", content: "one" }, preview: "{}", truncated: false },
          },
          { type: "finish", finishReason: "tool-calls" },
        ],
        runTool: async (request) => {
          await new Promise<void>((resolve) => {
            if (request.abortSignal.aborted) {
              resolve();
              return;
            }
            request.abortSignal.addEventListener("abort", () => resolve(), { once: true });
          });
          return { type: "cancelled" };
        },
      }))),
    );
    interruptOwner = () => {
      void Effect.runPromise(Fiber.interrupt(runFiber));
    };
    await waitForCondition(() => requestEnds.length === 1, "successful request-end before cooperative cancellation");
    await new Promise<void>((resolve) => setImmediate(resolve));
    failRepairWrite = true;
    session.state.beginCooperativeCancel();
    interruptOwner();
    const runExit = await Effect.runPromise(Fiber.await(runFiber));
    expect(Exit.isFailure(runExit) && Cause.hasInterruptsOnly(runExit.cause)).toBe(true);
    expect(repairAttempted).toBe(true);
    expect(requestEnds).toHaveLength(1);
    expect(requestEnds[0]).toMatchObject({
      isError: false,
      consumedAttachmentRefs: ["att_post_success_cooperative_failure"],
    });
  });

  test("post-success interrupt-fence failure settles the attachment ride already consumed by Bridge", async () => {
    const session = new Session("sesn_1");
    const attachment = {
      transient: {
        attachmentRef: "att_post_success_interrupt_failure",
        sourceToolUseEventId: "sevt_post_success_interrupt_failure",
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
      type: "messages",
      messages: [userMessage("user-post-success-interrupt-failure", 0, "run the tool")],
    });
    const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
    const baseWriter = writerFrom((envelope) => ({
      ok: true,
      writeId: envelope.writeId,
      eventId: envelope.event.type === "agent.tool_use"
        ? "sevt_post_success_interrupt_failure"
        : `bridge-${envelope.writeId}`,
      processedAt: createdAt,
    }));
    const writer: SessionEventWriter = {
      ...baseWriter,
      writeRequestEnd: async (envelope) => {
        requestEnds.push(envelope);
        session.state.beginUserInterrupt({
          ...acceptedInput("rin_post_success_interrupt_failure"),
          eventIds: ["sevt_post_success_interrupt_failure"],
          sequenceFrom: 9,
          sequenceTo: 9,
        }, async () => ({
          ok: false,
          retryable: false,
          errorCode: "interrupt_conflict",
        }));
        return await baseWriter.writeRequestEnd(envelope);
      },
    };

    const result = await Effect.runPromiseExit(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        writer,
        providerCallRuntime: {
          systemInstructions: "post-success interrupt-fence failure test",
          toolCatalog: catalogForTest({ name: "Write", description: "Write file", inputSchema: { type: "object" } }),
        },
        events: [
          {
            type: "tool-call",
            id: "tool-post-success-interrupt-failure",
            toolName: "Write",
            input: { file_path: "src/failure.ts", content: "one" },
            inputPreview: { value: { file_path: "src/failure.ts", content: "one" }, preview: "{}", truncated: false },
          },
          { type: "finish", finishReason: "tool-calls" },
        ],
        runTool: async (request) => {
          await new Promise<void>((resolve) => {
            if (request.abortSignal.aborted) {
              resolve();
              return;
            }
            request.abortSignal.addEventListener("abort", () => resolve(), { once: true });
          });
          return { type: "cancelled" };
        },
      }))),
    );

    expect(Exit.isFailure(result)).toBe(true);
    expect(requestEnds).toHaveLength(1);
    expect(requestEnds[0]).toMatchObject({
      isError: false,
      consumedAttachmentRefs: ["att_post_success_interrupt_failure"],
    });
    expect(session.state.pendingAttachments()).toEqual([]);
  });

  test("SessionManager joins the original interrupt FinishIdle ACK before releasing the run slot", async () => {
    const firstProviderStarted = deferred<void>();
    const releaseFinishIdle = deferred<void>();
    const finishIdleStarted = deferred<void>();
    const requests: LLMRequest[] = [];
    const finishIdleWriteIds: string[] = [];
    const loader = new QueuedContextLoader([], []);
    const baseWriter = writerFrom((envelope) => ({
      ok: true,
      writeId: envelope.writeId,
      eventId: `bridge-${envelope.writeId}`,
      processedAt: createdAt,
    }));
    const writer: SessionEventWriter = {
      ...baseWriter,
      finishIdle: async (envelope) => {
        finishIdleWriteIds.push(envelope.durableTurnId);
        if (finishIdleWriteIds.length === 1) {
          finishIdleStarted.resolve();
          await releaseFinishIdle.promise;
        }
        return withFinishIdleReceiptForTest(envelope, {
          ok: true,
          writeId: envelope.durableTurnId,
          eventId: `bridge-${envelope.durableTurnId}`,
          processedAt: createdAt,
        });
      },
    };
    let providerCalls = 0;
    const agentLayer = runtimeAgentLoopLayer(loader, {
      writer,
      llmService: {
        stream(request, streamOptions) {
          requests.push(request);
          providerCalls++;
          if (providerCalls === 1) {
            firstProviderStarted.resolve();
            return Stream.fromAsyncIterable((async function* () {
              await new Promise<void>((resolve) => {
                if (streamOptions?.abortSignal?.aborted === true) {
                  resolve();
                  return;
                }
                streamOptions?.abortSignal?.addEventListener("abort", () => resolve(), { once: true });
              });
            })(), (error): LLMServiceError => ({
              type: "llm-service",
              error: runtimeFailureFromProviderError(normalizeProviderError({
                code: "provider_stream_error",
                message: String(error),
                retryable: true,
              })),
            }));
          }
          return Stream.fromIterable([
            { type: "text-start" as const, id: "follow-up" },
            { type: "text-delta" as const, id: "follow-up", text_delta: "after ACK" },
            { type: "text-end" as const, id: "follow-up" },
            { type: "finish" as const, finishReason: "stop" as const },
          ]);
        },
      },
    });
    const managerLayer = SessionManager.layer({ maxLocalSessions: 4, now: () => createdAt }).pipe(Layer.provide(agentLayer));
    const { manager, scope } = await Effect.runPromise(Effect.gen(function* () {
      const layerScope = yield* Scope.make();
      const context = yield* Layer.buildWithScope(managerLayer, layerScope);
      return { manager: Context.get(context, SessionManager.Service), scope: layerScope };
    }));

    try {
      const firstInput = { ...acceptedInput("rin_finish_idle_owner"), sequenceFrom: 1, sequenceTo: 1 };
      await Effect.runPromise(manager.preloadThread({
        ...firstInput,
        runtimeBindingToken: "runtime-binding-token",
        coldCoverage: emptyColdCoverage,
        messages: [userMessage("user-finish-idle-owner", 0, "hold the first run")],
        thread: { role: "main", visibility: "public", agentType: "general", status: "idle" },
      }));
      await Effect.runPromise(manager.acceptInput(firstInput));
      await firstProviderStarted.promise;
      const interruptCommand = { ...acceptedInput("rin_finish_idle_interrupt"), sequenceFrom: 9, sequenceTo: 9 };
      let interruptSettled = false;
      const interrupt = Effect.runPromise(manager.interruptControl(
        "sesn_1",
        interruptCommand,
        testControlCommit(interruptCommand),
      )).then((result) => {
        interruptSettled = true;
        return result;
      });
      await finishIdleStarted.promise;
      const postFenceInput = { ...acceptedInput("rin_after_finish_idle"), sequenceFrom: 10, sequenceTo: 10 };
      await Effect.runPromise(manager.acceptInput(postFenceInput));
      await flushMicrotasks(50);

      expect(interruptSettled).toBe(false);
      expect(await Effect.runPromise(manager.waitThread(interruptCommand, 0))).toMatchObject({ ok: true, timedOut: true });
      expect(finishIdleWriteIds).toHaveLength(1);
      expect(new Set(finishIdleWriteIds).size).toBe(1);
      expect(providerCalls).toBe(1);

      releaseFinishIdle.resolve();
      await expect(interrupt).resolves.toMatchObject({ ok: true, interrupted: true });
      await Effect.runPromise(manager.waitThread(postFenceInput, 1_000));

      expect(providerCalls).toBe(2);
      expect(requests).toHaveLength(2);
      expect(loader.commitCalls.map((input) => input.runtimeInputId)).toEqual([
        "rin_finish_idle_owner",
        "rin_after_finish_idle",
      ]);
      expect(finishIdleWriteIds[0]).not.toBe(finishIdleWriteIds.at(-1));
    } finally {
      releaseFinishIdle.resolve();
      await Effect.runPromise(Scope.close(scope, Exit.void));
    }
  });

  test("interrupt accepted during ordinary FinishIdle completes after that idle ACK", async () => {
    const session = new Session({
      workspaceId: "wksp_test",
      sessionId: "sesn_1",
      sessionThreadId: "thrd_1",
      threadRole: "main",
      bindingId: "bind_1",
      bindingGeneration: 1,
      targetPodUid: "pod_1",
      runtimeBindingToken: "runtime-binding-token",
    });
    const releaseFinishIdle = deferred<void>();
    const finishIdleStarted = deferred<void>();
    const appended: SessionEvent[] = [];
    const finishIdleWriteIds: string[] = [];
    const loader = new RecordingContextLoader([], {
      type: "messages",
      messages: [userMessage("user-ordinary-finish-idle", 0, "finish this turn")],
    });
    const baseWriter = writerFrom((envelope) => {
      appended.push(envelope.event);
      return {
        ok: true,
        writeId: envelope.writeId,
        eventId: `bridge-${envelope.writeId}`,
        processedAt: createdAt,
      };
    });
    const writer: SessionEventWriter = {
      ...baseWriter,
      finishIdle: async (envelope) => {
        finishIdleWriteIds.push(envelope.durableTurnId);
        finishIdleStarted.resolve();
        await releaseFinishIdle.promise;
        return withFinishIdleReceiptForTest(envelope, {
          ok: true,
          writeId: envelope.durableTurnId,
          eventId: `bridge-${envelope.durableTurnId}`,
          processedAt: createdAt,
        });
      },
    };
    const agentLayer = runtimeAgentLoopLayer(loader, {
      writer,
      llmService: {
        stream() {
          return Stream.fromIterable([
            { type: "text-start" as const, id: "answer" },
            { type: "text-delta" as const, id: "answer", text_delta: "done" },
            { type: "text-end" as const, id: "answer" },
            { type: "finish" as const, finishReason: "stop" as const },
          ]);
        },
      },
    });
    const custody = testRunCustody();
    const runFiber = Effect.runFork(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, custody);
      }).pipe(Effect.provide(agentLayer)),
    );

    try {
      await finishIdleStarted.promise;

      const command = { ...acceptedInput("rin_interrupt_ordinary_finish_idle"), sequenceFrom: 9, sequenceTo: 9 };
      let commits = 0;
      expect(session.state.beginUserInterrupt(command, async (declaration) => {
        commits += 1;
        return buildRuntimeControlCommitResult(command, "interrupt_control", declaration);
      })).toBe("applied");
      const interrupted = Effect.runPromise(Fiber.interrupt(runFiber));
      expect(commits).toBe(0);

      releaseFinishIdle.resolve();
      await interrupted;
      const runExit = await Effect.runPromise(Fiber.await(runFiber));
      expect(Exit.isFailure(runExit) && Cause.hasInterruptsOnly(runExit.cause)).toBe(true);
      expect(commits).toBe(1);
      expect(session.state.userInterruptCloseoutCompleted(command.runtimeInputId)).toBe(true);
      expect(finishIdleWriteIds).toHaveLength(1);
      expect(appended.filter((event) => event.type === "session.error")).toEqual([]);
    } finally {
      releaseFinishIdle.resolve();
      await Effect.runPromise(Fiber.interrupt(runFiber));
    }
  });

  test("user interrupt repairs a committed ToolFiber before CommitInputs and FinishIdle", async () => {
    const session = new Session("sesn_1");
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "remember this")] });
    const releaseProvider = deferred<void>();
    const projectionFailureSeen = deferred<void>();
    const appended: SessionEvent[] = [];
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
              input: { action: "create", path: "notes/todo.md", content: "one" },
              inputPreview: { value: { action: "create", path: "notes/todo.md", content: "one" }, preview: "{\"action\":\"create\"}", truncated: false },
            };
            if (options?.abortSignal === undefined) {
              throw new Error("provider stream requires an abort signal");
            }
            await waitForReleaseOrAbort(releaseProvider.promise, options.abortSignal);
            yield { type: "finish" as const, finishReason: "tool-calls" as const };
          })(),
          (error): LLMServiceError => ({
            type: "llm-service",
            error: runtimeFailureFromProviderError(normalizeProviderError({
              code: "provider_stream_error",
              message: String(error),
              retryable: true,
            })),
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
          writeId: envelope.writeId,
          eventId: envelope.event.type === "agent.tool_use" ? "sevt_memory_projection_cancel" : `bridge-${envelope.writeId}`,
          processedAt: createdAt,
        };
      },
      async (envelope) => {
        requestEnds.push(envelope);
        closeoutOrder.push("event:span.model_request_end");
        return {
          ok: true,
          writeId: envelope.writeId,
          eventId: `bridge-${envelope.writeId}`,
          processedAt: createdAt,
        };
      },
    );
    const runFiber = Effect.runFork(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        writer,
        llmService: service,
        providerCallRuntime: {
          systemInstructions: "memory projection cancellation ToolFiber test",
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
            request.abortSignal.addEventListener("abort", () => resolve({ type: "cancelled" }), { once: true });
            projectionFailureSeen.resolve(undefined);
          });
        },
      }))),
    );

    await projectionFailureSeen.promise;
    expect(appended.filter((event) => event.type === "agent.tool_result")).toHaveLength(0);
    expect(JSON.stringify(appended)).not.toContain("projection_refresh_failed");
    const pendingToolPart = session.state.contextManager.messages().at(-1)?.parts.find((part) => part.type === "tool");
    expect(pendingToolPart?.type === "tool" ? pendingToolPart.state.status : undefined).toBe("running");
    const interruptCommand = {
      requestId: "req_interrupt",
      workspaceId: "wksp_test",
      sessionId: "sesn_1",
      sessionThreadId: "thrd_sesn_1",
      bindingId: "bind_sesn_1",
      bindingGeneration: 1,
      targetPodUid: "pod_sesn_1",
      runtimeInputId: "rin_interrupt",
      eventIds: ["sevt_interrupt"],
      sequenceFrom: 9,
      sequenceTo: 9,
    } as const;
    session.state.beginUserInterrupt(interruptCommand, async (declaration) => {
      closeoutOrder.push("commit:interrupt");
      return buildRuntimeControlCommitResult(interruptCommand, "interrupt_control", declaration);
    });
    const interrupt = Effect.runPromise(Fiber.interrupt(runFiber));
    await waitForCondition(() => observedToolSignal?.aborted === true, "Memory projection ToolFiber abort");
    releaseProvider.resolve(undefined);
    await interrupt;
    const runExit = await Effect.runPromise(Fiber.await(runFiber));
    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(Exit.isFailure(runExit) && Cause.hasInterruptsOnly(runExit.cause)).toBe(true);
    expect(toolRunnerCalls).toBe(1);
    expect(bridgeAttempts).toBe(1);
    expect(appended.filter((event) => event.type === "agent.tool_result")).toEqual([]);
    expect(appended.filter((event) => event.type === "session.error")).toEqual([]);
    expect(requestEnds).toHaveLength(1);
    const cancellationDraft = requestEnds[0]?.drafts?.find((draft) => draft.draftKind === "cancellation");
    expect(cancellationDraft).toBeDefined();
    expect(requestEnds[0]?.interruptSettlement).toEqual({
      runtimeInputId: "rin_interrupt",
      eventIds: ["sevt_interrupt"],
      sequenceFrom: 9,
      sequenceTo: 9,
      pendingToolCancellations: [],
      sandboxExecutionToolUseEventIds: [],
    });
    expect(requestEnds[0]?.drafts?.filter((draft) => draft.draftKind === "cancellation")).toEqual([
      expect.objectContaining({
        sourceKind: "interrupt_control",
        sourceId: "rin_interrupt",
        sourceEventId: "sevt_interrupt",
        parts: [expect.objectContaining({
          type: "tool",
          toolUseEventId: "sevt_memory_projection_cancel",
          state: expect.objectContaining({ status: "cancelled" }),
        })],
      }),
    ]);
    expect(closeoutOrder).not.toContain("commit:interrupt");
    expect(closeoutOrder.indexOf("event:span.model_request_end")).toBeLessThan(closeoutOrder.indexOf("event:session.status_idle"));
    expect(
      session.state.contextManager.messages().flatMap((message) => message.parts).find(
        (part) => part.type === "tool" && part.toolUseEventId === "sevt_memory_projection_cancel" && part.state.status === "cancelled",
      ),
    ).toBeDefined();
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
    const commitReceipt = (input: RuntimeAcceptedInputState): AcceptedInputCommitResult => {
      const result = acceptedInputReceipt(input, "committed", nextMessageSequence);
      nextMessageSequence += result.receipt.messages.length;
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
      eventSequence: 100_000,
      messageSequence: 100_000,
    };
    const store = new AgentLoopRuntimeStore(storeOrder, false, false, undefined, async (repair) => {
      if (repair.modelToolCallId === "tool-uncommitted") {
        uncommittedRepairStarted.resolve();
        await releaseUncommittedRepair.promise;
      }
    }, durableSequence);
    let providerCalls = 0;
    let toolUseWrites = 0;
    let toolRunnerCalls = 0;
    let interruptDeclaration: RuntimeControlInputDeclaration | undefined;
    const terminalCatalog = catalogForTest({ name: "Read", description: "Terminal read", inputSchema: { type: "object" } });
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
    const mixedWriterBase = writerFrom((envelope) => {
      appended.push(envelope.event);
      if (envelope.event.type === "agent.tool_result") {
        order.push(`event:agent.tool_result:${envelope.event.tool_use_id}`);
      } else if (envelope.event.type === "session.status_idle") {
        order.push(`event:session.status_idle:${envelope.event.stop_reason.type}`);
      } else {
        order.push(`event:${envelope.event.type}`);
      }
      if (envelope.event.type === "agent.tool_use") {
        toolUseWrites++;
      }
      if (envelope.event.type === "agent.tool_result" && envelope.event.tool_use_id === "sevt_mixed_tool_1") {
        terminalResultAcked.resolve();
      }
      if (envelope.event.type === "span.model_request_end") {
        requestEndObserved = true;
      }
      return {
        ok: true,
        writeId: envelope.writeId,
        eventId: envelope.event.type === "agent.tool_use" ? `sevt_mixed_tool_${toolUseWrites}` : `bridge-${envelope.writeId}`,
        processedAt: createdAt,
      };
    }, undefined, [], durableSequence);
    const mixedWriter: SessionEventWriter = {
      ...mixedWriterBase,
      append: async (envelope) => {
        const result = await mixedWriterBase.append(envelope);
        if (envelope.event.type === "agent.tool_use" && toolUseWrites === 2) {
          pendingToolUseAppendStarted.resolve();
          await releasePendingToolUseAppend.promise;
        }
        return result;
      },
    };
    const agentLayer = runtimeAgentLoopLayer(loader, {
      store,
      writer: mixedWriter,
      llmService: {
        stream() {
          providerCalls++;
          if (providerCalls > 1) {
            return Stream.fromIterable([
              { type: "text-start" as const, id: "follow-up" },
              { type: "text-delta" as const, id: "follow-up", text_delta: "continued" },
              { type: "text-end" as const, id: "follow-up" },
              { type: "finish" as const, finishReason: "stop" as const },
            ]);
          }
          return Stream.fromAsyncIterable((async function* () {
            yield {
              type: "tool-call" as const,
              id: "tool-terminal",
              toolName: "Read",
              input: { file_path: "src/shared.ts", content: "terminal" },
              inputPreview: { value: { file_path: "src/shared.ts", content: "terminal" }, preview: "{}", truncated: false },
            };
            await releaseNextProviderTool.promise;
            yield {
              type: "tool-call" as const,
              id: "tool-running",
              toolName: "Write",
              input: { file_path: "src/shared.ts", content: "running" },
              inputPreview: { value: { file_path: "src/shared.ts", content: "running" }, preview: "{}", truncated: false },
            };
            await pendingToolUseAppendStarted.promise;
            yield {
              type: "tool-call" as const,
              id: "tool-uncommitted",
              toolName: "UncommittedWrite",
              input: { file_path: "src/shared.ts", content: "must-not-commit" },
              inputPreview: { value: { file_path: "src/shared.ts", content: "must-not-commit" }, preview: "{}", truncated: false },
            };
            yield { type: "finish" as const, finishReason: "tool-calls" as const };
          })(), (error): LLMServiceError => ({
            type: "llm-service",
            error: runtimeFailureFromProviderError(normalizeProviderError({
              code: "provider_stream_error",
              message: String(error),
              retryable: true,
            })),
          }));
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
        return { type: "completed", output: { text: "terminal output", truncated: false } };
      },
    });
    const managerLayer = SessionManager.layer({ maxLocalSessions: 4, now: () => createdAt }).pipe(Layer.provide(agentLayer));
    const { manager, scope } = await Effect.runPromise(Effect.gen(function* () {
      const layerScope = yield* Scope.make();
      const context = yield* Layer.buildWithScope(managerLayer, layerScope);
      return { manager: Context.get(context, SessionManager.Service), scope: layerScope };
    }));

    try {
      const initialInput = acceptedInput("rin_initial_mixed");
      await Effect.runPromise(manager.preloadThread({
        ...initialInput,
        runtimeBindingToken: "runtime-binding-token",
        coldCoverage: emptyColdCoverage,
        messages: [],
        thread: { role: "main", visibility: "public", agentType: "general", status: "idle" },
      }));
      await Effect.runPromise(manager.acceptInput(initialInput));
      await terminalResultAcked.promise;
      let terminalToolApplied = false;
      for (let attempt = 0; attempt < 100 && !terminalToolApplied; attempt++) {
        const snapshot = await Effect.runPromise(manager.inspectThread(initialInput));
        terminalToolApplied =
          JSON.stringify(snapshot).includes("sevt_mixed_tool_1") &&
          JSON.stringify(snapshot).includes("terminal output");
        if (!terminalToolApplied) {
          await new Promise<void>((resolve) => setImmediate(resolve));
        }
      }
      expect(terminalToolApplied).toBe(true);
      releaseNextProviderTool.resolve();
      await pendingToolUseAppendStarted.promise;
      expect(appended.filter((event) => event.type === "agent.tool_use")).toHaveLength(2);
      releasePendingToolUseAppend.resolve();
      await uncommittedRepairStarted.promise;
      expect(appended.filter((event) => event.type === "agent.tool_result" && event.tool_use_id === "sevt_mixed_tool_2")).toHaveLength(0);
      expect(store.repairs.filter((repair) => repair.modelToolCallId === "tool-uncommitted")).toHaveLength(0);
      const preFenceInput = { ...acceptedInput("rin_pre_fence_mixed"), sequenceFrom: 8, sequenceTo: 8 };
      await Effect.runPromise(manager.acceptInput(preFenceInput));
      expect(commitCalls).toEqual(["rin_initial_mixed"]);
      releaseUncommittedRepair.resolve();
      try {
        await waitForCondition(
          () => requestEndObserved && preCommitObserved,
          "request-end and pre-fence input commit",
        );
      } catch {
        throw new Error(
          `request-end/pre-fence gate failed: requestEnd=${requestEndObserved}, preCommit=${preCommitObserved}, order=${order.join(",")}, store=${storeOrder.join(",")}, events=${JSON.stringify(appended)}`,
        );
      }
      expect(providerCalls).toBe(1);
      expect(toolRunnerCalls).toBe(1);
      expect(appended.filter((event) => event.type === "agent.tool_use")).toHaveLength(2);
      expect(appended.filter((event) => event.type === "agent.tool_result" && event.tool_use_id === "sevt_mixed_tool_1")).toHaveLength(1);
      expect(appended.filter((event) => event.type === "agent.tool_result" && event.tool_use_id === "sevt_mixed_tool_2")).toHaveLength(0);
      expect(JSON.stringify(appended)).not.toContain("tool-uncommitted");
      expect(JSON.stringify(appended)).not.toContain("must-not-commit");
      expect(store.repairs.filter((repair) => repair.modelToolCallId === "tool-uncommitted")).toHaveLength(1);
      expect(order).toContain("event:session.status_idle:requires_action");

      const interruptCommand = {
        ...acceptedInput("rin_mixed_interrupt"),
        eventIds: ["sevt_mixed_interrupt"],
        sequenceFrom: 9,
        sequenceTo: 9,
      };
      const interrupt = Effect.runPromise(manager.interruptControl("sesn_1", interruptCommand, async (declaration) => {
        interruptDeclaration = declaration;
        order.push("commit:interrupt");
        return buildRuntimeControlCommitResult(interruptCommand, "interrupt_control", declaration);
      }));
      await new Promise<void>((resolve) => setImmediate(resolve));
      await flushMicrotasks();
      const postFenceInput = { ...acceptedInput("rin_post_fence_mixed"), sequenceFrom: 10, sequenceTo: 10 };
      await Effect.runPromise(manager.acceptInput(postFenceInput));
      await flushMicrotasks();
      expect(order).not.toContain("commit:interrupt");
      expect(order).not.toContain("commit:rin_post_fence_mixed");
      expect(order.filter((entry) => entry === "event:agent.tool_result:sevt_mixed_tool_2")).toHaveLength(0);

      releasePreCommit.resolve();
      releaseNextProviderTool.resolve();
      const interruptResult = await interrupt;
      expect({ interruptResult, order }).toMatchObject({ interruptResult: { ok: true, interrupted: true } });
      await Effect.runPromise(manager.waitThread(postFenceInput, 1_000));

      expect(providerCalls).toBe(2);
      expect(toolRunnerCalls).toBe(1);
      expect(commitCalls).toEqual(["rin_initial_mixed", "rin_pre_fence_mixed", "rin_post_fence_mixed"]);
      expect(appended.filter((event) => event.type === "agent.tool_use")).toHaveLength(2);
      expect(appended.filter((event) => event.type === "agent.tool_result" && event.tool_use_id === "sevt_mixed_tool_1")).toHaveLength(1);
      expect(appended.filter((event) => event.type === "agent.tool_result" && event.tool_use_id === "sevt_mixed_tool_2")).toEqual([]);
      expect(interruptDeclaration?.pendingToolCancellations).toEqual([
        expect.objectContaining({
          toolUseEventId: "sevt_mixed_tool_2",
          runtimeLocalId: expect.any(String),
        }),
      ]);
      expect(interruptDeclaration?.pendingToolCancellations[0]?.runtimeLocalId)
        .toBe(interruptDeclaration?.drafts[0]?.runtimeLocalId);
      expect(interruptDeclaration?.drafts).toEqual([
        expect.objectContaining({
          draftKind: "cancellation",
          sourceKind: "interrupt_control",
          sourceId: "rin_mixed_interrupt",
          parts: [expect.objectContaining({
            type: "tool",
            toolUseEventId: "sevt_mixed_tool_2",
            state: expect.objectContaining({ status: "cancelled" }),
          })],
        }),
      ]);
      expect(JSON.stringify(appended)).not.toContain("tool-uncommitted");
      expect(JSON.stringify(appended)).not.toContain("must-not-commit");
      expect(appended.filter((event) => event.type === "session.error")).toEqual([]);
      expect(order.indexOf("commit:pre:end")).toBeLessThan(order.indexOf("commit:interrupt"));
      expect(order.indexOf("commit:interrupt")).toBeLessThan(order.indexOf("event:session.status_idle:end_turn"));
      expect(order.indexOf("event:session.status_idle:end_turn")).toBeLessThan(order.indexOf("commit:rin_post_fence_mixed"));
      const closeoutIdleIndex = order.indexOf("event:session.status_idle:end_turn");
      expect(order.slice(0, closeoutIdleIndex).filter((entry) => entry.startsWith("commit:"))).toEqual([
        "commit:initial",
        "commit:pre:start",
        "commit:pre:end",
        "commit:interrupt",
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
    const lateRoute = deferred<AgentLoop.RuntimeToolExecutionResult>();
    const order: string[] = [];
    const commitCalls: string[] = [];
    let nextMessageSequence = 1;
    const commitReceipt = (input: RuntimeAcceptedInputState): AcceptedInputCommitResult => {
      const result = acceptedInputReceipt(input, "committed", nextMessageSequence);
      nextMessageSequence += result.receipt.messages.length;
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
    const storeOrder: string[] = [];
    const store = new AgentLoopRuntimeStore(storeOrder);
    let providerCalls = 0;
    let toolRunnerCalls = 0;
    let toolUseWrites = 0;
    let observedRouteSignal: AbortSignal | undefined;
    let interruptDeclaration: RuntimeControlInputDeclaration | undefined;
    const agentLayer = runtimeAgentLoopLayer(loader, {
      store,
      writer: writerFrom((envelope) => {
        appended.push(envelope.event);
        if (envelope.event.type === "agent.tool_result") {
          order.push(`event:agent.tool_result:${envelope.event.tool_use_id}`);
        } else if (envelope.event.type === "session.status_idle") {
          order.push(`event:session.status_idle:${envelope.event.stop_reason.type}`);
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
          writeId: envelope.writeId,
          eventId: envelope.event.type === "agent.tool_use" ? `sevt_non_cooperative_route_${toolUseWrites}` : `bridge-${envelope.writeId}`,
          processedAt: createdAt,
        };
      }),
      llmService: {
        stream() {
          providerCalls++;
          order.push(`provider:${providerCalls}`);
          if (providerCalls > 1) {
            return Stream.fromIterable([
              { type: "text-start" as const, id: "follow-up" },
              { type: "text-delta" as const, id: "follow-up", text_delta: "continued" },
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
              inputPreview: { value: { file_path: "src/non-cooperative.ts", content: "late" }, preview: "{}", truncated: false },
            },
            { type: "finish" as const, finishReason: "tool-calls" as const },
          ]);
        },
      },
      providerCallRuntime: {
        systemInstructions: "non-cooperative post-stream ToolFiber test",
        toolCatalog: catalogForTest({ name: "Write", description: "Write file", inputSchema: { type: "object" } }),
      },
      runTool: (request) => {
        toolRunnerCalls++;
        observedRouteSignal = request.abortSignal;
        request.abortSignal.addEventListener("abort", () => abortObserved.resolve(), { once: true });
        routeStarted.resolve();
        return lateRoute.promise;
      },
    });
    const managerLayer = SessionManager.layer({ maxLocalSessions: 4, now: () => createdAt }).pipe(Layer.provide(agentLayer));
    const { manager, scope } = await Effect.runPromise(Effect.gen(function* () {
      const layerScope = yield* Scope.make();
      const context = yield* Layer.buildWithScope(managerLayer, layerScope);
      return { manager: Context.get(context, SessionManager.Service), scope: layerScope };
    }));

    try {
      const initialInput = acceptedInput("rin_non_cooperative_route");
      await Effect.runPromise(manager.preloadThread({
        ...initialInput,
        runtimeBindingToken: "runtime-binding-token",
        coldCoverage: emptyColdCoverage,
        messages: [],
        thread: { role: "main", visibility: "public", agentType: "general", status: "idle" },
      }));
      await Effect.runPromise(manager.acceptInput(initialInput));
      await Promise.all([routeStarted.promise, requestEndAcked.promise]);
      expect(toolRunnerCalls).toBe(1);
      expect(observedRouteSignal?.aborted).toBe(false);
      expect(appended.filter((event) => event.type === "agent.tool_use")).toHaveLength(1);
      expect(appended.filter((event) => event.type === "agent.tool_result")).toHaveLength(0);

      const interruptCommand = {
        ...acceptedInput("rin_non_cooperative_route_interrupt"),
        eventIds: ["sevt_non_cooperative_route_interrupt"],
        sequenceFrom: 9,
        sequenceTo: 9,
      };
      let interruptSettled = false;
      const interrupt = Effect.runPromise(manager.interruptControl("sesn_1", interruptCommand, async (declaration) => {
        interruptDeclaration = declaration;
        order.push("commit:interrupt");
        return buildRuntimeControlCommitResult(interruptCommand, "interrupt_control", declaration);
      })).then((result) => {
        interruptSettled = true;
        return result;
      });
      await new Promise<void>((resolve) => setImmediate(resolve));
      const postFenceInput = { ...acceptedInput("rin_after_non_cooperative_route"), sequenceFrom: 10, sequenceTo: 10 };
      await Effect.runPromise(manager.acceptInput(postFenceInput));
      await flushMicrotasks(50);

      expect(observedRouteSignal?.aborted).toBe(true);
      await abortObserved.promise;
      expect(interruptSettled).toBe(false);
      expect(providerCalls).toBe(1);
      expect(commitCalls).toEqual(["rin_non_cooperative_route"]);
      expect(order).not.toContain("commit:interrupt");
      expect(await Effect.runPromise(manager.waitThread(interruptCommand, 0))).toMatchObject({ ok: true, timedOut: true });

      await new Promise((resolve) => setTimeout(resolve, 700));
      expect(interruptSettled).toBe(true);
      await expect(interrupt).resolves.toMatchObject({ ok: true, interrupted: true });
      await Effect.runPromise(manager.waitThread(postFenceInput, 1_000));

      expect(appended.filter((event) => event.type === "agent.tool_result" && event.tool_use_id === "sevt_non_cooperative_route_1")).toEqual([]);
      expect(interruptDeclaration?.pendingToolCancellations).toEqual([]);
      expect(interruptDeclaration?.drafts).toEqual([
        expect.objectContaining({
          draftKind: "cancellation",
          sourceKind: "interrupt_control",
          sourceId: "rin_non_cooperative_route_interrupt",
          parts: [expect.objectContaining({
            type: "tool",
            toolUseEventId: "sevt_non_cooperative_route_1",
            state: expect.objectContaining({ status: "cancelled" }),
          })],
        }),
      ]);
      expect(appended.filter((event) => event.type === "session.error")).toEqual([]);
      expect(order.indexOf("commit:interrupt")).toBeLessThan(order.indexOf("event:session.status_idle:end_turn"));
      expect(order.indexOf("event:session.status_idle:end_turn")).toBeLessThan(order.indexOf("commit:rin_after_non_cooperative_route"));
      expect(order.indexOf("commit:rin_after_non_cooperative_route")).toBeLessThan(order.indexOf("provider:2"));
      expect(providerCalls).toBe(2);
      expect(toolRunnerCalls).toBe(1);

      const eventCountAtCloseout = appended.length;
      const storeOperationCountAtCloseout = storeOrder.length;
      const snapshotAtCloseout = await Effect.runPromise(manager.inspectThread(postFenceInput));
      lateRoute.resolve({
        type: "completed",
        output: { text: "late non-cooperative output", truncated: false },
        attachments: [{
          transient: {
            attachmentRef: "att_late_non_cooperative",
            sourceToolUseEventId: "sevt_non_cooperative_route_1",
            sourcePath: "tool:late-non-cooperative.png",
            pageRange: "",
            detail: "auto",
          },
          fileBacked: undefined,
          mime: "image/png",
          filename: "late-non-cooperative.png",
        }],
        backgroundTask: { taskId: "task_late_non_cooperative" },
      });
      await flushMicrotasks(50);

      expect(appended).toHaveLength(eventCountAtCloseout);
      expect(storeOrder).toHaveLength(storeOperationCountAtCloseout);
      expect(providerCalls).toBe(2);
      expect(toolRunnerCalls).toBe(1);
      expect(await Effect.runPromise(manager.inspectThread(postFenceInput))).toEqual(snapshotAtCloseout);
      expect(JSON.stringify(snapshotAtCloseout)).not.toContain("late non-cooperative output");
      expect(JSON.stringify(snapshotAtCloseout)).not.toContain("att_late_non_cooperative");
      expect(JSON.stringify(snapshotAtCloseout)).not.toContain("task_late_non_cooperative");
    } finally {
      jest.useRealTimers();
      lateRoute.resolve({
        type: "completed",
        output: { text: "cleanup", truncated: false },
        attachments: [{
          transient: {
            attachmentRef: "att_cleanup",
            sourceToolUseEventId: "sevt_non_cooperative_route_1",
            sourcePath: "tool:cleanup.png",
            pageRange: "",
            detail: "auto",
          },
          fileBacked: undefined,
          mime: "image/png",
          filename: "cleanup.png",
        }],
        backgroundTask: { taskId: "task_cleanup" },
      });
      await Effect.runPromise(Scope.close(scope, Exit.void));
    }
  });

  test("SessionManager interrupts rehydrated approved tools, repairs every open sibling, and preserves a pre-fence settlement", async () => {
    const loadedMessage = DurableRuntimeMessageSchema.parse({
      id: "assistant-rehydrated-approved",
      sessionId: "sesn_1",
      owningEventId: "sevt_approved_settled",
      eventSequence: 2,
      role: "assistant",
      origin: "agent",
      sequence: 1,
      status: "completed",
      createdAt,
      parts: [
        {
          id: "part-approved-settled",
          sessionId: "sesn_1",
          messageId: "assistant-rehydrated-approved",
          sequence: 0,
          type: "tool",
          toolCallId: "tool-approved-settled",
          toolName: "Write",
          toolUseEventId: "sevt_approved_settled",
          toolEvent: { kind: "tool" },
          state: {
            status: "running",
            input: { value: { file_path: "src/settled.ts", content: "settled" }, preview: "{}", truncated: false },
          },
          startedAt: createdAt,
          createdAt,
        },
        {
          id: "part-approved-late",
          sessionId: "sesn_1",
          messageId: "assistant-rehydrated-approved",
          sequence: 1,
          type: "tool",
          toolCallId: "tool-approved-late",
          toolName: "Write",
          toolUseEventId: "sevt_approved_late",
          toolEvent: { kind: "tool" },
          state: {
            status: "running",
            input: { value: { file_path: "src/late.ts", content: "late" }, preview: "{}", truncated: false },
          },
          startedAt: createdAt,
          createdAt,
        },
      ],
    });
    const pendingToolUses = [
      {
        toolUseEventId: "sevt_approved_settled",
        modelRequestId: "mrq_rehydrated_approved",
        modelToolCallId: "tool-approved-settled",
        toolName: "Write",
        kind: "approval" as const,
        input: { file_path: "src/settled.ts", content: "settled" },
        status: "resolving" as const,
        decision: "allow" as const,
        expiresAt: "2026-06-14T00:30:00.000Z",
      },
      {
        toolUseEventId: "sevt_approved_late",
        modelRequestId: "mrq_rehydrated_approved",
        modelToolCallId: "tool-approved-late",
        toolName: "Write",
        kind: "approval" as const,
        input: { file_path: "src/late.ts", content: "late" },
        status: "resolving" as const,
        decision: "allow" as const,
        expiresAt: "2026-06-14T00:30:00.000Z",
      },
    ];
    const loader = new QueuedContextLoader([], []);
    const lateRoute = deferred<AgentLoop.RuntimeToolExecutionResult>();
    const lateRouteStarted = deferred<void>();
    const settledResultAcked = deferred<void>();
    const abortObserved = deferred<void>();
    const appended: SessionEvent[] = [];
    const order: string[] = [];
    const storeOrder: string[] = [];
    const durableSequence: TestDurableSequence = {
      eventSequence: 100_000,
      messageSequence: 100_000,
    };
    const store = new AgentLoopRuntimeStore(storeOrder, false, false, undefined, undefined, durableSequence);
    let observedLateSignal: AbortSignal | undefined;
    let providerCalls = 0;
    const agentLayer = runtimeAgentLoopLayer(loader, {
      store,
      writer: writerFrom((envelope) => {
        appended.push(envelope.event);
        order.push(envelope.event.type === "agent.tool_result"
          ? `event:agent.tool_result:${envelope.event.tool_use_id}`
          : `event:${envelope.event.type}`);
        if (envelope.event.type === "agent.tool_result" && envelope.event.tool_use_id === "sevt_approved_settled") {
          settledResultAcked.resolve();
        }
        return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
      }, undefined, [{
        sessionThreadId: "thrd_1",
        message: loadedMessage,
      }], durableSequence),
      llmService: {
        stream() {
          providerCalls++;
          return Stream.empty;
        },
      },
      providerCallRuntime: {
        systemInstructions: "rehydrated approved tool interrupt test",
        toolCatalog: catalogForTest({ name: "Write", description: "Write file", inputSchema: { type: "object" }, permissionPolicy: "always_ask" }),
      },
      runTool: (request) => {
        if (request.modelToolCallId === "tool-approved-settled") {
          return { type: "completed", output: { text: "settled before interrupt", truncated: false } };
        }
        observedLateSignal = request.abortSignal;
        request.abortSignal.addEventListener("abort", () => abortObserved.resolve(), { once: true });
        lateRouteStarted.resolve();
        return lateRoute.promise;
      },
    });
    const managerLayer = SessionManager.layer({ maxLocalSessions: 4, now: () => createdAt }).pipe(Layer.provide(agentLayer));
    const { manager, scope } = await Effect.runPromise(Effect.gen(function* () {
      const layerScope = yield* Scope.make();
      const context = yield* Layer.buildWithScope(managerLayer, layerScope);
      return { manager: Context.get(context, SessionManager.Service), scope: layerScope };
    }));
    let interrupt: Promise<unknown> | undefined;
    let interruptDeclaration: RuntimeControlInputDeclaration | undefined;

    try {
      const input = acceptedInput("rin_rehydrated_approved");
      await Effect.runPromise(manager.preloadThread({
        ...input,
        runtimeBindingToken: "runtime-binding-token",
        messages: [userMessage("user-rehydrated-approved", 0, "resume approved tools"), loadedMessage],
        pendingToolUses,
        coldCoverage: {
          ...emptyColdCoverage,
          pendingToolIds: pendingToolUses.map((pending) => pending.toolUseEventId),
        },
        thread: { role: "main", visibility: "public", agentType: "general", status: "idle" },
      }));
      await Effect.runPromise(manager.acceptInput(input));
      await Promise.all([lateRouteStarted.promise, settledResultAcked.promise]);
      const interruptCommand = {
        ...acceptedInput("rin_rehydrated_approved_interrupt"),
        eventIds: ["sevt_rehydrated_approved_interrupt"],
        sequenceFrom: 9,
        sequenceTo: 9,
      };
      let interruptSettled = false;
      interrupt = Effect.runPromise(manager.interruptControl("sesn_1", interruptCommand, async (declaration) => {
        interruptDeclaration = declaration;
        order.push("commit:interrupt");
        return buildRuntimeControlCommitResult(interruptCommand, "interrupt_control", declaration);
      }));
      void interrupt.then(() => {
        interruptSettled = true;
      });
      await new Promise<void>((resolve) => setImmediate(resolve));
      await flushMicrotasks();

      expect(observedLateSignal?.aborted).toBe(true);
      await abortObserved.promise;
      await new Promise((resolve) => setTimeout(resolve, 700));
      expect(interruptSettled).toBe(true);
      expect(await interrupt).toMatchObject({ ok: true, interrupted: true });

      const settledResults = appended.filter((event) => event.type === "agent.tool_result" && event.tool_use_id === "sevt_approved_settled");
      const repairedResults = appended.filter((event) => event.type === "agent.tool_result" && event.tool_use_id === "sevt_approved_late");
      expect(settledResults).toHaveLength(1);
      expect(settledResults[0]).not.toMatchObject({ is_error: true });
      expect(repairedResults).toEqual([]);
      expect(interruptDeclaration?.pendingToolCancellations).toEqual([
        expect.objectContaining({
          toolUseEventId: "sevt_approved_late",
          runtimeLocalId: expect.any(String),
        }),
      ]);
      expect(interruptDeclaration?.pendingToolCancellations[0]?.runtimeLocalId)
        .toBe(interruptDeclaration?.drafts[0]?.runtimeLocalId);
      expect(interruptDeclaration?.drafts).toEqual([
        expect.objectContaining({
          draftKind: "cancellation",
          sourceKind: "interrupt_control",
          sourceId: "rin_rehydrated_approved_interrupt",
          parts: [expect.objectContaining({
            type: "tool",
            toolUseEventId: "sevt_approved_late",
            state: expect.objectContaining({ status: "cancelled" }),
          })],
        }),
      ]);
      expect(appended.filter((event) => event.type === "session.error")).toEqual([]);
      expect(order.indexOf("commit:interrupt")).toBeLessThan(order.indexOf("event:session.status_idle"));
      expect(providerCalls).toBe(0);

      const eventCountAtCloseout = appended.length;
      const storeOperationCountAtCloseout = storeOrder.length;
      const snapshotAtCloseout = await Effect.runPromise(manager.inspectThread(interruptCommand));
      lateRoute.resolve({
        type: "completed",
        output: { text: "late approved output", truncated: false },
        attachments: [{
          transient: {
            attachmentRef: "att_late_approved",
            sourceToolUseEventId: "sevt_approved_late",
            sourcePath: "tool:late-approved.png",
            pageRange: "",
            detail: "auto",
          },
          fileBacked: undefined,
          mime: "image/png",
          filename: "late-approved.png",
        }],
        backgroundTask: { taskId: "task_late_approved" },
      });
      await flushMicrotasks(50);

      expect(appended).toHaveLength(eventCountAtCloseout);
      expect(storeOrder).toHaveLength(storeOperationCountAtCloseout);
      expect(providerCalls).toBe(0);
      expect(await Effect.runPromise(manager.inspectThread(interruptCommand))).toEqual(snapshotAtCloseout);
      expect(JSON.stringify(snapshotAtCloseout)).not.toContain("late approved output");
      expect(JSON.stringify(snapshotAtCloseout)).not.toContain("att_late_approved");
      expect(JSON.stringify(snapshotAtCloseout)).not.toContain("task_late_approved");
    } finally {
      jest.useRealTimers();
      lateRoute.resolve({
        type: "completed",
        output: { text: "cleanup", truncated: false },
        attachments: [{
          transient: {
            attachmentRef: "att_cleanup",
            sourceToolUseEventId: "sevt_approved_late",
            sourcePath: "tool:cleanup.png",
            pageRange: "",
            detail: "auto",
          },
          fileBacked: undefined,
          mime: "image/png",
          filename: "cleanup.png",
        }],
        backgroundTask: { taskId: "task_cleanup" },
      });
      if (interrupt !== undefined) {
        await interrupt.catch(() => undefined);
      }
      await Effect.runPromise(Scope.close(scope, Exit.void));
    }
  });

  test("interrupt snapshot joins an in-flight pre-fence CommitInputs and remains the last closeout input", async () => {
    const preCommitStarted = deferred<void>();
    const releasePreCommit = deferred<void>();
    const order: string[] = [];
    const commitCalls: string[] = [];
    let nextMessageSequence = 1;
    const commitReceipt = (input: RuntimeAcceptedInputState): AcceptedInputCommitResult => {
      const result = acceptedInputReceipt(input, "committed", nextMessageSequence);
      nextMessageSequence += result.receipt.messages.length;
      return result;
    };
    const loader: TestContextLoader = {
      buildContext: async () => [],
      loadPendingInput: async () => ({ type: "empty" }),
      commitAcceptedInput: async (input) => {
        commitCalls.push(input.runtimeInputId);
        if (input.runtimeInputId === "rin_pre_fence") {
          order.push("commit:pre:start");
          preCommitStarted.resolve();
          await releasePreCommit.promise;
          order.push("commit:pre:end");
          return commitReceipt(input);
        }
        order.push(`commit:${input.runtimeInputId}`);
        return commitReceipt(input);
      },
    };
    const appended: SessionEvent[] = [];
    const managerLayer = SessionManager.layer({ maxLocalSessions: 4, now: () => createdAt }).pipe(
      Layer.provide(runtimeAgentLoopLayer(loader, {
        writer: writerFrom((envelope) => {
          appended.push(envelope.event);
          order.push(`event:${envelope.event.type}`);
          return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
        }),
      })),
    );
    const { manager, scope } = await Effect.runPromise(Effect.gen(function* () {
      const layerScope = yield* Scope.make();
      const context = yield* Layer.buildWithScope(managerLayer, layerScope);
      return { manager: Context.get(context, SessionManager.Service), scope: layerScope };
    }));

    try {
      const preFenceInput = { ...acceptedInput("rin_pre_fence"), sequenceFrom: 5, sequenceTo: 5 };
      await Effect.runPromise(manager.preloadThread({
        ...preFenceInput,
        runtimeBindingToken: "runtime-binding-token",
        coldCoverage: emptyColdCoverage,
        messages: [],
        thread: { role: "main", visibility: "public", agentType: "general", status: "idle" },
      }));
      await Effect.runPromise(manager.acceptInput(preFenceInput));
      await preCommitStarted.promise;
      const interruptCommand = { ...acceptedInput("rin_commit_fence_interrupt"), sequenceFrom: 9, sequenceTo: 9 };
      const interrupt = Effect.runPromise(manager.interruptControl("sesn_1", interruptCommand, async (declaration) => {
        order.push("commit:interrupt");
        return buildRuntimeControlCommitResult(interruptCommand, "interrupt_control", declaration);
      }));
      await new Promise<void>((resolve) => setImmediate(resolve));
      await Effect.runPromise(manager.acceptInput({ ...acceptedInput("rin_post_fence"), sequenceFrom: 10, sequenceTo: 10 }));
      await new Promise((resolve) => setTimeout(resolve, 10));
      expect(order).toEqual(["event:session.status_running", "commit:pre:start"]);

      releasePreCommit.resolve();
      await expect(interrupt).resolves.toMatchObject({ ok: true, interrupted: true });
      await Effect.runPromise(manager.waitThread(interruptCommand, 1_000));

      expect(commitCalls).toEqual(["rin_pre_fence", "rin_post_fence"]);
      expect(order.indexOf("commit:pre:end")).toBeLessThan(order.indexOf("commit:interrupt"));
      expect(order.indexOf("commit:interrupt")).toBeLessThan(order.indexOf("event:session.status_idle"));
      expect(order.indexOf("event:session.status_idle")).toBeLessThan(order.indexOf("commit:rin_post_fence"));
      expect(appended.filter((event) => event.type === "session.error")).toEqual([]);
    } finally {
      releasePreCommit.resolve();
      await Effect.runPromise(Scope.close(scope, Exit.void));
    }
  });

  test("runtime shutdown joins durable Sandbox acceptance before freezing execution ownership", async () => {
    const session = new Session("sesn_1");
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const acceptanceStarted = deferred<void>();
    const releaseAcceptance = deferred<void>();
    let awaitCalls = 0;
    const catalog = catalogForTest({ name: "Write", description: "Write file", inputSchema: { type: "object" } });
    const runFiber = Effect.runFork(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        events: [
          {
            type: "tool-call",
            id: "tool-accept-fence",
            toolName: "Write",
            input: { file_path: "src/a.ts", content: "one" },
            inputPreview: { value: { file_path: "src/a.ts", content: "one" }, preview: "{}", truncated: false },
          },
          { type: "finish", finishReason: "tool-calls" },
        ],
        providerCallRuntime: {
          systemInstructions: "sandbox acceptance fence test",
          toolCatalog: {
            ...catalog,
            entries: catalog.entries.map((entry) => ({
              ...entry,
              route: { kind: "sandbox" as const, operation: "RunTool" as const, helperSubcommand: "write" as const },
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
          return { type: "completed", output: { text: "must not wait after shutdown", truncated: false } };
        },
      }))),
    );

    await acceptanceStarted.promise;
    session.state.beginRuntimeShutdown();
    let interruptSettled = false;
    const interrupted = Effect.runPromise(Fiber.interrupt(runFiber)).then((exit) => {
      interruptSettled = true;
      return exit;
    });
    await flushMicrotasks(20);
    expect(interruptSettled).toBe(false);

    releaseAcceptance.resolve();
    await interrupted;

    expect(awaitCalls).toBe(0);
    expect(session.state.pendingSandboxExecutionJobs()).toHaveLength(1);
    expect(session.state.pendingApprovalToolJobs()).toHaveLength(0);
  });

  test("runtime shutdown aborts active ToolFiber route execution", async () => {
    const session = new Session("sesn_1");
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
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
              inputPreview: { value: { file_path: "src/a.ts", content: "one" }, preview: "{\"file_path\":\"src/a.ts\"}", truncated: false },
            };
            if (options?.abortSignal === undefined) {
              throw new Error("provider stream requires an abort signal");
            }
            await waitForReleaseOrAbort(releaseProvider.promise, options.abortSignal);
            yield { type: "finish" as const, finishReason: "tool-calls" as const };
          })(),
          (error): LLMServiceError => ({
            type: "llm-service",
            error: runtimeFailureFromProviderError(normalizeProviderError({
              code: "provider_stream_error",
              message: String(error),
              retryable: true,
            })),
          }),
        );
      },
    };

    const runFiber = Effect.runFork(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
            llmService: service,
            writer: writerFrom((envelope) => {
              order.push(`event:${envelope.event.type}`);
              return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
            }),
            providerCallRuntime: {
              systemInstructions: "tool cancellation test system",
              toolCatalog: catalogForTest({ name: "Write", description: "Write file", inputSchema: { type: "object" } }),
            },
            runTool: (request) => {
              observedToolSignal = request.abortSignal;
              return new Promise((resolve) => {
                request.abortSignal.addEventListener("abort", () => {
                  order.push("tool-abort-signalled");
                  void releaseTool.promise.then(() => {
                    order.push("tool-route-settled");
                    resolve({ type: "completed" as const, output: { text: "late", truncated: false } });
                  });
                }, { once: true });
              });
            },
          }),
        ),
      ),
    );

    await waitForCondition(() => observedToolSignal !== undefined, "tool route signal");
    session.state.beginRuntimeShutdown();
    const shutdown = Effect.runPromise(Fiber.interrupt(runFiber));
    await waitForCondition(() => observedToolSignal?.aborted === true, "tool route abort");
    releaseProvider.resolve(undefined);
    await new Promise((resolve) => setTimeout(resolve, 25));
    releaseTool.resolve(undefined);
    await shutdown;
    const runExit = await Effect.runPromise(Fiber.await(runFiber));

    expect(Exit.isFailure(runExit) && Cause.hasInterruptsOnly(runExit.cause)).toBe(true);
    expect(order.indexOf("tool-abort-signalled")).toBeGreaterThan(-1);
    expect(order).not.toContain("event:span.model_request_end");
    expect(order).not.toContain("event:agent.tool_result");
  });

  test("approve_for_me runs reviewer before public tool_use is written", async () => {
    const session = new Session("sesn_1", new AutoApprovalReviewerManager());
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const order: string[] = [];
    const appended: SessionEvent[] = [];
    const writer = writerFrom((envelope) => {
      order.push(`event:${envelope.event.type}`);
      appended.push(envelope.event);
      return { ok: true, writeId: envelope.writeId, eventId: envelope.event.type === "agent.tool_use" ? "bridge-tool" : "bridge-event", processedAt: createdAt };
    });

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
            writer,
            approvalMode: "approve_for_me",
            events: [
              {
                type: "tool-call",
                id: "tool-1",
                toolName: "Write",
                input: { file_path: "src/a.ts", content: "ok" },
                inputPreview: { value: { file_path: "src/a.ts", content: "ok" }, preview: "{\"file_path\":\"src/a.ts\"}", truncated: false },
              },
              { type: "finish", finishReason: "tool-calls" },
            ],
            providerCallRuntime: {
              systemInstructions: "approval reviewer test system",
              toolCatalog: catalogForTest({ name: "Write", description: "Write file", inputSchema: { type: "object" }, permissionPolicy: "always_ask" }),
            },
            reviewApproval: (request) => Effect.sync(() => {
              order.push(`review:${request.targetModelToolCallId}`);
              expect(appended.some((event) => event.type === "agent.tool_use")).toBe(false);
              return { type: "decision" as const, riskLevel: "low" as const, userAuthorization: "high" as const, outcome: "allow" as const };
            }),
            runTool: () => {
              order.push("run-tool");
              return { type: "completed", output: { text: "done", truncated: false } };
            },
          }),
        ),
      ),
    );

    expect(result).toMatchObject({ type: "completed" });
    expect(order.indexOf("review:tool-1")).toBeLessThan(order.indexOf("event:agent.tool_use"));
    expect(order.indexOf("event:agent.tool_use")).toBeLessThan(order.indexOf("run-tool"));
    expect(appended).toContainEqual(expect.objectContaining({
      type: "agent.tool_use",
      evaluated_permission: "allow",
    }));
  });

  test("approve_for_me reviewer failure falls back to public ask approval", async () => {
    const session = new Session("sesn_1", new AutoApprovalReviewerManager());
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const appended: SessionEvent[] = [];
    let runToolCalls = 0;
    let toolUseIndex = 0;
    const writer = writerFrom((envelope) => {
      appended.push(envelope.event);
      return {
        ok: true,
        writeId: envelope.writeId,
        eventId: envelope.event.type === "agent.tool_use" ? `sevt_tool_${++toolUseIndex}` : `bridge-${envelope.writeId}`,
        processedAt: createdAt,
      };
    });

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
            writer,
            approvalMode: "approve_for_me",
            events: [
              {
                type: "tool-call",
                id: "tool-1",
                toolName: "Write",
                input: { file_path: "src/a.ts", content: "ok" },
                inputPreview: { value: { file_path: "src/a.ts", content: "ok" }, preview: "{\"file_path\":\"src/a.ts\"}", truncated: false },
              },
              { type: "finish", finishReason: "tool-calls" },
            ],
            providerCallRuntime: {
              systemInstructions: "approval reviewer fallback test system",
              toolCatalog: catalogForTest({ name: "Write", description: "Write file", inputSchema: { type: "object" }, permissionPolicy: "always_ask" }),
            },
            reviewApproval: () => Effect.succeed({ type: "failed" }),
            runTool: () => {
              runToolCalls += 1;
              return { type: "completed", output: { text: "should not run", truncated: false } };
            },
          }),
        ),
      ),
    );

    expect(result).toMatchObject({ type: "completed" });
    expect(runToolCalls).toBe(0);
    expect(appended).toContainEqual(expect.objectContaining({
      type: "agent.tool_use",
      evaluated_permission: "ask",
    }));
    expect(appended.at(-1)).toEqual({
      type: "session.status_idle",
      stop_reason: { type: "requires_action", event_ids: ["sevt_tool_1"] },
    });
  });

  test("approval reviewer stale custody stops the turn and discards HotState", async () => {
    const session = new Session("sesn_1", new AutoApprovalReviewerManager());
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const appended: SessionEvent[] = [];
    const writer = writerFrom((envelope) => {
      appended.push(envelope.event);
      return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
    });

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
            writer,
            approvalMode: "approve_for_me",
            events: [
              {
                type: "tool-call",
                id: "tool-1",
                toolName: "Write",
                input: { file_path: "src/a.ts", content: "ok" },
                inputPreview: { value: { file_path: "src/a.ts", content: "ok" }, preview: "{\"file_path\":\"src/a.ts\"}", truncated: false },
              },
              { type: "finish", finishReason: "tool-calls" },
            ],
            providerCallRuntime: {
              systemInstructions: "approval reviewer stale custody test system",
              toolCatalog: catalogForTest({ name: "Write", description: "Write file", inputSchema: { type: "object" }, permissionPolicy: "always_ask" }),
            },
            reviewApproval: () => Effect.succeed({ type: "stale_custody" as const }),
            runTool: () => ({ type: "completed", output: { text: "must not run", truncated: false } }),
          }),
        ),
      ),
    );

    expect(result).toEqual({ type: "interrupted", discardHotState: true });
    expect(appended.some((event) => event.type === "agent.tool_use")).toBe(false);
  });

  test("approve_for_me reviewer receives current request-turn draft state", async () => {
    const session = new Session("sesn_1", new AutoApprovalReviewerManager());
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const order: string[] = [];
    const appended: SessionEvent[] = [];
    const writer = writerFrom((envelope) => {
      order.push(`event:${envelope.event.type}`);
      appended.push(envelope.event);
      return { ok: true, writeId: envelope.writeId, eventId: envelope.event.type === "agent.tool_use" ? "bridge-tool" : "bridge-event", processedAt: createdAt };
    });

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
            writer,
            approvalMode: "approve_for_me",
            events: [
              { type: "text-start", id: "text-1" },
              { type: "text-delta", id: "text-1", text_delta: "I will update the file before calling the tool." },
              { type: "text-end", id: "text-1" },
              { type: "reasoning-start", id: "reasoning-1" },
              { type: "reasoning-delta", id: "reasoning-1", text_delta: "Need to patch one file." },
              { type: "reasoning-end", id: "reasoning-1" },
              {
                type: "tool-call",
                id: "tool-1",
                toolName: "Write",
                input: { file_path: "src/a.ts", content: "ok" },
                inputPreview: { value: { file_path: "src/a.ts", content: "ok" }, preview: "{\"file_path\":\"src/a.ts\"}", truncated: false },
              },
              { type: "finish", finishReason: "tool-calls" },
            ],
            providerCallRuntime: {
              systemInstructions: "approval reviewer test system",
              toolCatalog: catalogForTest({ name: "Write", description: "Write file", inputSchema: { type: "object" }, permissionPolicy: "always_ask" }),
            },
            reviewApproval: (request) => Effect.sync(() => {
              order.push(`review:${request.targetModelToolCallId}`);
              expect(appended.some((event) => event.type === "agent.tool_use")).toBe(false);
              const currentDraft = request.currentRequestTurnMessages.find((message) => message.role === "assistant" && message.status === "streaming");
              expect(currentDraft?.parts).toEqual(expect.arrayContaining([
                expect.objectContaining({ type: "text", text: "I will update the file before calling the tool.", status: "completed" }),
                expect.objectContaining({ type: "reasoning", text: "Need to patch one file.", status: "completed" }),
                expect.objectContaining({ type: "tool", toolCallId: "tool-1", toolName: "Write", state: expect.objectContaining({ status: "running" }) }),
              ]));
              return { type: "decision" as const, riskLevel: "low" as const, userAuthorization: "high" as const, outcome: "allow" as const };
            }),
            runTool: () => ({ type: "completed", output: { text: "done", truncated: false } }),
          }),
        ),
      ),
    );

    expect(result).toMatchObject({ type: "completed" });
    expect(order.indexOf("review:tool-1")).toBeLessThan(order.indexOf("event:agent.tool_use"));
  });

  test("ask approval resumes the pending ToolJob instead of rerunning the old ToolFiber", async () => {
    const session = new Session("sesn_1");
    const loader = new QueuedContextLoader([], [
      { type: "messages", messages: [userMessage("user-1", 0, "hello")] },
      { type: "empty" },
    ]);
    const appended: SessionEvent[] = [];
    const toolUseEventIds: string[] = [];
    let toolUseIndex = 0;
    const writer = writerFrom((envelope) => {
      appended.push(envelope.event);
      const eventId = envelope.event.type === "agent.tool_use" ? `sevt_tool_${++toolUseIndex}` : `bridge-${envelope.writeId}`;
      if (envelope.event.type === "agent.tool_use") {
        toolUseEventIds.push(eventId);
      }
      return { ok: true, writeId: envelope.writeId, eventId, processedAt: createdAt };
    });
    const requests: LLMRequest[] = [];
    const runToolCalls: string[] = [];
    const sandboxAcceptanceCalls: string[] = [];
    const processors: SessionProcessor[] = [];
    const store = new AgentLoopRuntimeStore([]);
    const layer = runtimeAgentLoopLayer(loader, {
      store,
      writer,
      llmService: queuedLLMService([
        [
          {
            type: "tool-call",
            id: "tool-1",
            toolName: "Write",
            input: { file_path: "src/a.ts", content: "ok" },
            inputPreview: { value: { file_path: "src/a.ts", content: "ok" }, preview: "{\"file_path\":\"src/a.ts\"}", truncated: false },
          },
          { type: "finish", finishReason: "tool-calls" },
        ],
        [
          { type: "text-start", id: "text-1" },
          { type: "text-delta", id: "text-1", text_delta: "after approval" },
          { type: "text-end", id: "text-1" },
          { type: "finish", finishReason: "stop" },
        ],
      ], requests),
      providerCallRuntime: {
        systemInstructions: "approval resume test system",
        toolCatalog: {
          ...catalogForTest({ name: "Write", description: "Write file", inputSchema: { type: "object" }, permissionPolicy: "always_ask" }),
          entries: [{
            ...catalogForTest({ name: "Write", description: "Write file", inputSchema: { type: "object" }, permissionPolicy: "always_ask" }).entries[0]!,
            route: { kind: "sandbox" as const, operation: "RunTool" as const, helperSubcommand: "write" as const },
          }],
        },
      },
      runTool: (request) => {
        runToolCalls.push(`${request.modelToolCallId}:${request.toolUseEventId}`);
        return { type: "completed", output: { text: "approved write", truncated: false } };
      },
      acceptSandboxExecution: (request) => {
        sandboxAcceptanceCalls.push(`${request.modelToolCallId}:${request.toolUseEventId}`);
        expect(session.state.pendingApprovalToolJobs()).toHaveLength(1);
        expect(session.state.pendingSandboxExecutionJobs()).toHaveLength(0);
        return { type: "accepted" };
      },
      createProcessor: (options) => {
        const processor = new SessionProcessor(options);
        processors.push(processor);
        return processor;
      },
    });

    const first = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(layer)),
    );

    expect(first).toMatchObject({ type: "completed" });
    expect(toolUseEventIds).toEqual(["sevt_tool_1"]);
    expect(runToolCalls).toEqual([]);
    expect(sandboxAcceptanceCalls).toEqual([]);
    expect(processors).toHaveLength(1);
    const pendingApproval = session.state.pendingApprovalToolJobs()[0];
    expect(pendingApproval?.assistantMessage.role).toBe("assistant");
    expect(pendingApproval?.assistantMessage.status).toBe("completed");
    expect(pendingApproval?.toolPart.type).toBe("tool");
    expect(pendingApproval?.toolPart.toolUseEventId).toBe("sevt_tool_1");
    expect(pendingApproval).not.toHaveProperty("processor");
    expect(appended.at(-1)).toEqual({
      type: "session.status_idle",
      stop_reason: { type: "requires_action", event_ids: ["sevt_tool_1"] },
    });
    expect(session.state.resolveToolConfirmation({
      requestId: "req_confirm",
      workspaceId: session.identity.workspaceId,
      sessionId: session.identity.sessionId,
      sessionThreadId: session.identity.sessionThreadId,
      bindingId: session.identity.bindingId,
      bindingGeneration: session.identity.bindingGeneration,
      targetPodUid: session.identity.targetPodUid,
      runtimeInputId: "rin_confirm",
      eventIds: ["sevt_confirm"],
      sequenceFrom: 2,
      sequenceTo: 2,
      sourceEventId: "sevt_confirm",
      toolUseEventId: "sevt_tool_1",
      decision: "allow",
    })).toBe("applied");

    const second = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(layer)),
    );

    expect(second).toMatchObject({ type: "completed" });
    expect(requests).toHaveLength(2);
    expect(sandboxAcceptanceCalls).toEqual(["tool-1:sevt_tool_1"]);
    expect(runToolCalls).toEqual(["tool-1:sevt_tool_1"]);
    expect(session.state.pendingApprovalToolJobs()).toHaveLength(0);
    expect(session.state.pendingSandboxExecutionJobs()).toHaveLength(0);
    expect(processors).toHaveLength(3);
    expect(processors[1]).not.toBe(processors[0]);
    expect(appended.map((event) => event.type)).toEqual([
      "session.status_running",
      "span.model_request_start",
      "span.model_request_end",
      "agent.tool_use",
      "session.status_idle",
      "session.status_running",
      "agent.tool_result",
      "span.model_request_start",
      "agent.message",
      "span.model_request_end",
      "session.status_idle",
    ]);
  });

  test("LoadContext pendingToolUses hydrates cold approval waits and settles the original tool use", async () => {
    const session = new Session("sesn_1");
    session.state.enqueueAcceptedInput(acceptedInput("rin_cold_restore"));
    const pendingInput = { file_path: "src/a.ts", content: "ok" };
    const pendingMessage = DurableRuntimeMessageSchema.parse({
      id: "assistant-cold-restore",
      sessionId: "sesn_1",
      owningEventId: "sevt_tool_1",
      eventSequence: 2,
      role: "assistant",
      origin: "agent",
      sequence: 1,
      status: "completed",
      createdAt,
      parts: [{
        id: "assistant-cold-restore-tool",
        sessionId: "sesn_1",
        messageId: "assistant-cold-restore",
        sequence: 0,
        type: "tool",
        toolCallId: "tool-1",
        toolName: "Write",
        toolUseEventId: "sevt_tool_1",
        toolEvent: { kind: "mcp", mcpServerName: "github" },
        state: {
          status: "running",
          input: {
            value: pendingInput,
            preview: "{\"file_path\":\"src/a.ts\",\"content\":\"ok\"}",
            truncated: false,
          },
        },
        startedAt: createdAt,
        createdAt,
      }],
    });
    const loadedMessages = [userMessage("user-cold", 0, "hello"), pendingMessage];
    const pendingToolUses = [{
      toolUseEventId: "sevt_tool_1",
      modelRequestId: "mrq_cold_restore",
      modelToolCallId: "tool-1",
      toolName: "Write",
      kind: "approval" as const,
      input: pendingInput,
      status: "pending" as const,
      expiresAt: "2026-06-14T00:30:00.000Z",
    }];
    const loader = new QueuedContextLoader([], []);
    const appended: SessionEvent[] = [];
    const writer = writerFrom(
      (envelope) => {
        appended.push(envelope.event);
        return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
      },
      undefined,
      [{ sessionThreadId: session.identity.sessionThreadId, message: pendingMessage }],
    );
    const requests: LLMRequest[] = [];
    const runToolCalls: string[] = [];
    const store = new AgentLoopRuntimeStore([]);
    const coldCatalog = catalogForTest({ name: "Write", description: "Write file", inputSchema: { type: "object" }, permissionPolicy: "always_ask" });
    const layer = runtimeAgentLoopLayer(loader, {
      store,
      writer,
      llmService: queuedLLMService([
        [
          { type: "text-start", id: "text-1" },
          { type: "text-delta", id: "text-1", text_delta: "after cold approval" },
          { type: "text-end", id: "text-1" },
          { type: "finish", finishReason: "stop" },
        ],
      ], requests),
      providerCallRuntime: {
        systemInstructions: "cold approval resume test system",
        toolCatalog: {
          ...coldCatalog,
          entries: coldCatalog.entries.map((entry) => ({
            ...entry,
            route: { kind: "gateway" as const, operation: "RunMcpTool" as const, mcpServerName: "github" },
          })),
        },
      },
      runTool: (request) => {
        runToolCalls.push(`${request.modelRequestId}:${request.modelToolCallId}:${request.toolUseEventId}`);
        expect(request.input).toEqual(pendingInput);
        return { type: "completed", output: { text: "cold approved write", truncated: false } };
      },
    });

    const first = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        session.state.contextManager.replaceMessages(loadedMessages);
        session.state.markPersistentContextLoaded();
        agentLoop.seedRuntimeModel(session);
        expect(yield* agentLoop.installLoadedPendingToolUses(session, pendingToolUses, loadedMessages)).toEqual({ ok: true });
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(layer)),
    );

    expect(first).toMatchObject({ type: "completed" });
    expect(loader.commitCalls.map((input) => input.runtimeInputId)).toEqual(["rin_cold_restore"]);
    expect(requests).toHaveLength(0);
    expect(runToolCalls).toEqual([]);
    expect(appended.at(-1)).toEqual({
      type: "session.status_idle",
      stop_reason: { type: "requires_action", event_ids: ["sevt_tool_1"] },
    });
    expect(session.state.contextManager.messages().find((message) => message.id === pendingMessage.id)?.parts[0]).toMatchObject({
      type: "tool",
      toolUseEventId: "sevt_tool_1",
      toolEvent: { kind: "mcp", mcpServerName: "github" },
      state: { status: "running" },
    });

    expect(session.state.resolveToolConfirmation({
      requestId: "req_confirm_cold",
      workspaceId: session.identity.workspaceId,
      sessionId: session.identity.sessionId,
      sessionThreadId: session.identity.sessionThreadId,
      bindingId: session.identity.bindingId,
      bindingGeneration: session.identity.bindingGeneration,
      targetPodUid: session.identity.targetPodUid,
      runtimeInputId: "rin_confirm_cold",
      eventIds: ["sevt_confirm_cold"],
      sequenceFrom: 2,
      sequenceTo: 2,
      sourceEventId: "sevt_confirm_cold",
      toolUseEventId: "sevt_tool_1",
      decision: "allow",
    })).toBe("applied");

    const second = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(layer)),
    );

    expect(second).toMatchObject({ type: "completed" });
    expect(requests).toHaveLength(1);
    expect(runToolCalls).toEqual(["mrq_cold_restore:tool-1:sevt_tool_1"]);
    expect(appended.map((event) => event.type)).toEqual([
      "session.status_running",
      "session.status_idle",
      "session.status_running",
      "agent.mcp_tool_result",
      "span.model_request_start",
      "agent.message",
      "span.model_request_end",
      "session.status_idle",
    ]);
  });

  test("LoadContext pendingSandboxExecutions rejoins the original durable Tool Use", async () => {
    const session = new Session("sesn_1");
    session.state.enqueueAcceptedInput(acceptedInput("rin_cold_sandbox_recovery"));
    const input = { file_path: "src/a.ts", content: "ok" };
    const durableToolMessage = DurableRuntimeMessageSchema.parse({
      id: "assistant-cold-sandbox",
      sessionId: "sesn_1",
      owningEventId: "sevt_sandbox_tool_1",
      eventSequence: 2,
      role: "assistant",
      origin: "agent",
      sequence: 1,
      status: "completed",
      createdAt,
      parts: [{
        id: "assistant-cold-sandbox-tool",
        sessionId: "sesn_1",
        messageId: "assistant-cold-sandbox",
        sequence: 0,
        type: "tool",
        toolCallId: "tool-sandbox-1",
        toolName: "Write",
        toolUseEventId: "sevt_sandbox_tool_1",
        toolEvent: { kind: "tool" },
        state: {
          status: "running",
          input: { value: input, preview: JSON.stringify(input), truncated: false },
        },
        startedAt: createdAt,
        createdAt,
      }],
    });
    const loadedMessages = [userMessage("user-cold-sandbox", 0, "hello"), durableToolMessage];
    const pendingSandboxExecutions = [{
      toolUseEventId: "sevt_sandbox_tool_1",
      modelRequestId: "mrq_cold_sandbox",
      modelToolCallId: "tool-sandbox-1",
      toolName: "Write",
      input,
      executionState: "running" as const,
    }];
    const loader = new QueuedContextLoader([], []);
    const appended: SessionEvent[] = [];
    const writer = writerFrom(
      (envelope) => {
        appended.push(envelope.event);
        if (envelope.event.type === "agent.tool_result") {
          sandboxResultDigests.push((envelope as unknown as { readonly sandboxResultDigest?: string }).sandboxResultDigest);
        }
        return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
      },
      undefined,
      [{ sessionThreadId: session.identity.sessionThreadId, message: durableToolMessage }],
    );
    const requests: LLMRequest[] = [];
    const runToolCalls: string[] = [];
    const sandboxResultDigests: Array<string | undefined> = [];
    let refreshAttempts = 0;
    const store = new AgentLoopRuntimeStore([]);
    const sandboxCatalog = catalogForTest({ name: "Write", description: "Write file", inputSchema: { type: "object" } });
    const layer = runtimeAgentLoopLayer(loader, {
      store,
      writer,
      llmService: queuedLLMService([[
        { type: "text-start", id: "text-1" },
        { type: "text-delta", id: "text-1", text_delta: "after sandbox recovery" },
        { type: "text-end", id: "text-1" },
        { type: "finish", finishReason: "stop" },
      ]], requests),
      providerCallRuntime: {
        systemInstructions: "cold sandbox recovery test system",
        toolCatalog: {
          ...sandboxCatalog,
          entries: sandboxCatalog.entries.map((entry) => ({
            ...entry,
            route: { kind: "sandbox" as const, operation: "RunTool" as const, helperSubcommand: "write" as const },
          })),
        },
      },
      runTool: (request) => {
        runToolCalls.push(`${request.modelRequestId}:${request.modelToolCallId}:${request.toolUseEventId}`);
        expect(refreshAttempts).toBe(2);
        expect(request.input).toEqual(input);
        return {
          type: "completed",
          output: { text: "cold sandbox write", truncated: false },
          sandboxResultDigest: "a".repeat(64),
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
        throw new Error("cold accepted Sandbox execution must not be accepted again");
      },
    });

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        session.state.contextManager.replaceMessages(loadedMessages);
        session.state.markPersistentContextLoaded();
        agentLoop.seedRuntimeModel(session);
        expect(yield* agentLoop.installLoadedSandboxExecutions(session, pendingSandboxExecutions, loadedMessages)).toEqual({ ok: true });
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(layer)),
    );

    expect(result).toMatchObject({ type: "completed" });
    expect(refreshAttempts).toBe(3);
    expect(sandboxResultDigests).toEqual(["a".repeat(64)]);
    expect(runToolCalls).toEqual(["mrq_cold_sandbox:tool-sandbox-1:sevt_sandbox_tool_1"]);
    expect(appended.filter((event) => event.type === "agent.tool_use")).toHaveLength(0);
    expect(appended.some((event) => event.type === "agent.tool_result")).toBe(true);
    expect(requests).toHaveLength(1);
  });

  test("cold accepted Sandbox execution releases stale Runtime custody without authoring a result", async () => {
    const session = new Session("sesn_1");
    session.state.enqueueAcceptedInput(acceptedInput("rin_cold_sandbox_stale_custody"));
    const input = { file_path: "src/a.ts", content: "ok" };
    const durableToolMessage = DurableRuntimeMessageSchema.parse({
      id: "assistant-cold-sandbox-stale",
      sessionId: "sesn_1",
      owningEventId: "sevt_sandbox_tool_stale",
      eventSequence: 2,
      role: "assistant",
      origin: "agent",
      sequence: 1,
      status: "completed",
      createdAt,
      parts: [{
        id: "assistant-cold-sandbox-stale-tool",
        sessionId: "sesn_1",
        messageId: "assistant-cold-sandbox-stale",
        sequence: 0,
        type: "tool",
        toolCallId: "tool-sandbox-stale",
        toolName: "Write",
        toolUseEventId: "sevt_sandbox_tool_stale",
        toolEvent: { kind: "tool" },
        state: { status: "running", input: { value: input, preview: JSON.stringify(input), truncated: false } },
        startedAt: createdAt,
        createdAt,
      }],
    });
    const loadedMessages = [userMessage("user-cold-sandbox-stale", 0, "hello"), durableToolMessage];
    const pendingSandboxExecutions = [{
      toolUseEventId: "sevt_sandbox_tool_stale",
      modelRequestId: "mrq_cold_sandbox_stale",
      modelToolCallId: "tool-sandbox-stale",
      toolName: "Write",
      input,
      executionState: "running" as const,
    }];
    const appended: SessionEvent[] = [];
    const requests: LLMRequest[] = [];
    let refreshAttempts = 0;
    const sandboxCatalog = catalogForTest({ name: "Write", description: "Write file", inputSchema: { type: "object" } });
    const layer = runtimeAgentLoopLayer(new QueuedContextLoader([], []), {
      writer: writerFrom(
        (envelope) => {
          appended.push(envelope.event);
          return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
        },
        undefined,
        [{ sessionThreadId: session.identity.sessionThreadId, message: durableToolMessage }],
      ),
      llmService: queuedLLMService([[{ type: "finish", finishReason: "stop" }]], requests),
      providerCallRuntime: {
        systemInstructions: "cold sandbox stale custody test system",
        toolCatalog: {
          ...sandboxCatalog,
          entries: sandboxCatalog.entries.map((entry) => ({
            ...entry,
            route: { kind: "sandbox" as const, operation: "RunTool" as const, helperSubcommand: "write" as const },
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
        throw new Error("cold accepted Sandbox execution must not be accepted again");
      },
    });

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        session.state.contextManager.replaceMessages(loadedMessages);
        session.state.markPersistentContextLoaded();
        agentLoop.seedRuntimeModel(session);
        expect(yield* agentLoop.installLoadedSandboxExecutions(session, pendingSandboxExecutions, loadedMessages)).toEqual({ ok: true });
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(layer)),
    );

    expect(result).toEqual({ type: "interrupted", discardHotState: true });
    expect(refreshAttempts).toBe(1);
    expect(appended.some((event) => event.type === "agent.tool_result")).toBe(false);
    expect(requests).toEqual([]);
  });

  test("cold unresolved approval does not strand an accepted Sandbox execution", async () => {
    const session = new Session("sesn_1");
    session.state.enqueueAcceptedInput(acceptedInput("rin_cold_mixed_recovery"));
    const approvalInput = { file_path: "src/approval.ts", content: "wait" };
    const sandboxInput = { file_path: "src/accepted.ts", content: "done" };
    const pendingMessage = DurableRuntimeMessageSchema.parse({
      id: "assistant-cold-mixed",
      sessionId: "sesn_1",
      owningEventId: "sevt_approval",
      eventSequence: 2,
      role: "assistant",
      origin: "agent",
      sequence: 1,
      status: "completed",
      createdAt,
      parts: [
        {
          id: "assistant-cold-mixed-approval",
          sessionId: "sesn_1",
          messageId: "assistant-cold-mixed",
          sequence: 0,
          type: "tool",
          toolCallId: "tool-approval",
          toolName: "Write",
          toolUseEventId: "sevt_approval",
          toolEvent: { kind: "tool" },
          state: { status: "running", input: { value: approvalInput, preview: JSON.stringify(approvalInput), truncated: false } },
          startedAt: createdAt,
          createdAt,
        },
        {
          id: "assistant-cold-mixed-sandbox",
          sessionId: "sesn_1",
          messageId: "assistant-cold-mixed",
          sequence: 1,
          type: "tool",
          toolCallId: "tool-sandbox",
          toolName: "Write",
          toolUseEventId: "sevt_sandbox",
          toolEvent: { kind: "tool" },
          state: { status: "running", input: { value: sandboxInput, preview: JSON.stringify(sandboxInput), truncated: false } },
          startedAt: createdAt,
          createdAt,
        },
      ],
    });
    const loadedMessages = [userMessage("user-cold-mixed", 0, "hello"), pendingMessage];
    const pendingToolUses = [{
      toolUseEventId: "sevt_approval",
      modelRequestId: "mrq_cold_mixed",
      modelToolCallId: "tool-approval",
      toolName: "Write",
      kind: "approval" as const,
      input: approvalInput,
      status: "pending" as const,
      expiresAt: "2026-06-14T00:30:00.000Z",
    }];
    const pendingSandboxExecutions = [{
      toolUseEventId: "sevt_sandbox",
      modelRequestId: "mrq_cold_mixed",
      modelToolCallId: "tool-sandbox",
      toolName: "Write",
      input: sandboxInput,
      executionState: "running" as const,
    }];
    const appended: SessionEvent[] = [];
    const writer = writerFrom(
      (envelope) => {
        appended.push(envelope.event);
        return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
      },
      undefined,
      [{ sessionThreadId: session.identity.sessionThreadId, message: pendingMessage }],
    );
    const coldCatalog = catalogForTest({ name: "Write", description: "Write file", inputSchema: { type: "object" }, permissionPolicy: "always_ask" });
    const sandboxCatalog = {
      ...coldCatalog,
      entries: coldCatalog.entries.map((entry) => ({
        ...entry,
        route: { kind: "sandbox" as const, operation: "RunTool" as const, helperSubcommand: "write" as const },
      })),
    };
    const waits: string[] = [];
    const layer = runtimeAgentLoopLayer(new QueuedContextLoader([], []), {
      writer,
      providerCallRuntime: { systemInstructions: "cold mixed recovery", toolCatalog: sandboxCatalog },
      acceptSandboxExecution: () => {
        throw new Error("accepted Sandbox execution must not be accepted again");
      },
      awaitSandboxExecution: (request) => {
        waits.push(request.toolUseEventId);
        return { type: "completed", output: { text: "recovered", truncated: false } };
      },
    });

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        session.state.contextManager.replaceMessages(loadedMessages);
        session.state.markPersistentContextLoaded();
        agentLoop.seedRuntimeModel(session);
        expect(yield* agentLoop.installLoadedPendingToolUses(session, pendingToolUses, loadedMessages)).toEqual({ ok: true });
        expect(yield* agentLoop.installLoadedSandboxExecutions(session, pendingSandboxExecutions, loadedMessages)).toEqual({ ok: true });
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(layer)),
    );

    expect(result).toMatchObject({ type: "completed" });
    expect(waits).toEqual(["sevt_sandbox"]);
    expect(session.state.pendingSandboxExecutionJobs()).toHaveLength(0);
    expect(session.state.pendingApprovalToolJobs()).toHaveLength(1);
    expect(appended.some((event) => event.type === "agent.tool_result" && event.tool_use_id === "sevt_sandbox")).toBe(true);
    expect(appended.at(-1)).toEqual({
      type: "session.status_idle",
      stop_reason: { type: "requires_action", event_ids: ["sevt_approval"] },
    });
  });

  test("LoadContext pendingToolUses applies recorded deny decisions without re-waiting or executing the tool", async () => {
    const session = new Session("sesn_1");
    session.state.enqueueAcceptedInput(acceptedInput("rin_cold_deny_restore"));
    const pendingInput = { file_path: "src/a.ts", content: "ok" };
    const loadedMessages = [
      userMessage("user-cold-deny", 0, "hello"),
      DurableRuntimeMessageSchema.parse({
        id: "assistant-cold-deny",
        sessionId: "sesn_1",
        owningEventId: "sevt_tool_1",
        eventSequence: 2,
        role: "assistant",
        origin: "agent",
        sequence: 1,
        status: "completed",
        createdAt,
        parts: [
          {
            id: "assistant-cold-deny-tool",
            sessionId: "sesn_1",
            messageId: "assistant-cold-deny",
            sequence: 0,
            type: "tool",
            toolCallId: "tool-1",
            toolName: "Write",
            toolUseEventId: "sevt_tool_1",
            toolEvent: { kind: "tool" },
            state: {
              status: "running",
              input: {
                value: pendingInput,
                preview: "{\"file_path\":\"src/a.ts\",\"content\":\"ok\"}",
                truncated: false,
              },
            },
            startedAt: createdAt,
            createdAt,
          },
        ],
      }),
    ];
    const pendingMessage = DurableRuntimeMessageSchema.parse(loadedMessages[1]);
    const pendingToolUses = [{
      toolUseEventId: "sevt_tool_1",
      modelRequestId: "mrq_cold_deny_restore",
      modelToolCallId: "tool-1",
      toolName: "Write",
      kind: "approval" as const,
      input: pendingInput,
      status: "resolving" as const,
      decision: "deny" as const,
      denyMessage: "not now",
      expiresAt: "2026-06-14T00:30:00.000Z",
    }];
    const loader = new QueuedContextLoader([], []);
    const appended: SessionEvent[] = [];
    const writer = writerFrom(
      (envelope) => {
        appended.push(envelope.event);
        return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
      },
      undefined,
      [{ sessionThreadId: session.identity.sessionThreadId, message: pendingMessage }],
    );
    const requests: LLMRequest[] = [];
    const store = new AgentLoopRuntimeStore([]);
    const layer = runtimeAgentLoopLayer(loader, {
      store,
      writer,
      llmService: queuedLLMService([
        [
          { type: "text-start", id: "text-1" },
          { type: "text-delta", id: "text-1", text_delta: "denied" },
          { type: "text-end", id: "text-1" },
          { type: "finish", finishReason: "stop" },
        ],
      ], requests),
      providerCallRuntime: {
        systemInstructions: "cold approval deny resume test system",
        toolCatalog: catalogForTest({ name: "Write", description: "Write file", inputSchema: { type: "object" }, permissionPolicy: "always_ask" }),
      },
      runTool: () => {
        throw new Error("denied pending approval must not execute the tool");
      },
    });

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        session.state.contextManager.replaceMessages(loadedMessages);
        session.state.markPersistentContextLoaded();
        agentLoop.seedRuntimeModel(session);
        expect(yield* agentLoop.installLoadedPendingToolUses(session, pendingToolUses, loadedMessages)).toEqual({ ok: true });
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(layer)),
    );

    expect(result).toMatchObject({ type: "completed" });
    expect(requests).toHaveLength(1);
    expect(appended.some((event) => event.type === "agent.tool_result")).toBe(true);
    expect(appended).not.toContainEqual({
      type: "session.status_idle",
      stop_reason: { type: "requires_action", event_ids: ["sevt_tool_1"] },
    });
  });

  test("partial approval keeps the RequestTurn idle until all waiting ToolJobs resolve", async () => {
    const session = new Session("sesn_1");
    const loader = new QueuedContextLoader([], [
      { type: "messages", messages: [userMessage("user-1", 0, "hello")] },
      { type: "empty" },
      { type: "empty" },
    ]);
    const appended: SessionEvent[] = [];
    const toolUseEventIds: string[] = [];
    let toolUseIndex = 0;
    const writer = writerFrom((envelope) => {
      appended.push(envelope.event);
      const eventId = envelope.event.type === "agent.tool_use" ? `sevt_tool_${++toolUseIndex}` : `bridge-${envelope.writeId}`;
      if (envelope.event.type === "agent.tool_use") {
        toolUseEventIds.push(eventId);
      }
      return { ok: true, writeId: envelope.writeId, eventId, processedAt: createdAt };
    });
    const requests: LLMRequest[] = [];
    const runToolCalls: string[] = [];
    const layer = runtimeAgentLoopLayer(loader, {
      writer,
      llmService: queuedLLMService([
        [
          {
            type: "tool-call",
            id: "tool-1",
            toolName: "Write",
            input: { file_path: "src/shared.ts", content: "one" },
            inputPreview: { value: { file_path: "src/shared.ts", content: "one" }, preview: "{\"file_path\":\"src/shared.ts\"}", truncated: false },
          },
          {
            type: "tool-call",
            id: "tool-2",
            toolName: "Write",
            input: { file_path: "/workspace/src/shared.ts", content: "two" },
            inputPreview: { value: { file_path: "/workspace/src/shared.ts", content: "two" }, preview: "{\"file_path\":\"/workspace/src/shared.ts\"}", truncated: false },
          },
          { type: "finish", finishReason: "tool-calls" },
        ],
        [
          { type: "text-start", id: "text-1" },
          { type: "text-delta", id: "text-1", text_delta: "all approved" },
          { type: "text-end", id: "text-1" },
          { type: "finish", finishReason: "stop" },
        ],
      ], requests),
      providerCallRuntime: {
        systemInstructions: "partial approval test system",
        toolCatalog: catalogForTest({ name: "Write", description: "Write file", inputSchema: { type: "object" }, permissionPolicy: "always_ask" }),
      },
      runTool: (request) => {
        runToolCalls.push(request.modelToolCallId);
        return { type: "completed", output: { text: `approved ${request.modelToolCallId}`, truncated: false } };
      },
    });

    const run = async () =>
      await Effect.runPromise(
        Effect.gen(function* () {
          const agentLoop = yield* AgentLoop.Service;
          return yield* agentLoop.run(session, testRunCustody());
        }).pipe(Effect.provide(layer)),
      );

    expect(await run()).toMatchObject({ type: "completed" });
    expect(toolUseEventIds).toEqual(["sevt_tool_1", "sevt_tool_2"]);
    const eventCountWhileBothApprovalsWait = appended.length;

    expect(session.state.resolveToolConfirmation({
      requestId: "req_confirm_1",
      workspaceId: session.identity.workspaceId,
      sessionId: session.identity.sessionId,
      sessionThreadId: session.identity.sessionThreadId,
      bindingId: session.identity.bindingId,
      bindingGeneration: session.identity.bindingGeneration,
      targetPodUid: session.identity.targetPodUid,
      runtimeInputId: "rin_confirm_1",
      eventIds: ["sevt_confirm_1"],
      sequenceFrom: 2,
      sequenceTo: 2,
      sourceEventId: "sevt_confirm_1",
      toolUseEventId: "sevt_tool_1",
      decision: "allow",
    })).toBe("applied");
    expect(await run()).toMatchObject({ type: "completed" });

    expect(requests).toHaveLength(1);
    expect(runToolCalls).toEqual([]);
    expect(appended).toHaveLength(eventCountWhileBothApprovalsWait);

    expect(session.state.resolveToolConfirmation({
      requestId: "req_confirm_2",
      workspaceId: session.identity.workspaceId,
      sessionId: session.identity.sessionId,
      sessionThreadId: session.identity.sessionThreadId,
      bindingId: session.identity.bindingId,
      bindingGeneration: session.identity.bindingGeneration,
      targetPodUid: session.identity.targetPodUid,
      runtimeInputId: "rin_confirm_2",
      eventIds: ["sevt_confirm_2"],
      sequenceFrom: 3,
      sequenceTo: 3,
      sourceEventId: "sevt_confirm_2",
      toolUseEventId: "sevt_tool_2",
      decision: "allow",
    })).toBe("applied");
    expect(await run()).toMatchObject({ type: "completed" });

    expect(requests).toHaveLength(2);
    expect(runToolCalls).toEqual(["tool-1", "tool-2"]);
  });

  test("runtime layer keeps unacked progress out of hot context and requests hot-state discard", async () => {
    const order: string[] = [];
    const session = new Session("sesn_1");
    const store = new AgentLoopRuntimeStore(order);
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const appendedTypes: string[] = [];
    const writer = failingEventWriter(appendedTypes, (event) => event.type === "agent.message");
    await installLoaderStateForTest(loader, session);

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          AgentLoop.runtimeLayer({
            internalToolRepairStore: store,
            sessionEventWriter: writer,
            runtime: agentLoopRuntime(),
            llmService: llmService([
              { type: "text-start", id: "text-1" },
              { type: "text-delta", id: "text-1", text_delta: "hello" },
              { type: "text-end", id: "text-1" },
            ]),
            storeOperationTimeoutMs: 1_000,
            providerCallRuntime: { ...DefaultProviderCallRuntimeConfig, timeoutMs: 1_800_000 },
            runtimeModel: () => ({ providerId: "fake", modelId: "fake-chat" }),
          }).pipe(Layer.provide(AgentLoop.contextLoaderLayer(loader))),
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
      "session.error",
      "session.status_idle",
    ]);
    expect(session.state.contextManager.messages().some((message) => message.role === "assistant")).toBe(false);
  });

  test("runtime layer treats progress append exhaustion as fatal and terminal append failures as non-recursive best effort", async () => {
    const order: string[] = [];
    const session = new Session("sesn_1");
    const store = new AgentLoopRuntimeStore(order);
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const appendedTypes: string[] = [];
    const writer = failingEventWriter(appendedTypes, (event) =>
      event.type === "agent.message" || event.type === "session.error" || event.type === "session.status_idle");

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
            store,
            writer,
            events: [
              { type: "text-start", id: "text-1" },
              { type: "text-delta", id: "text-1", text_delta: "hello" },
              { type: "text-end", id: "text-1" },
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
      "session.error",
      "session.status_idle",
    ]);
    expect(session.state.contextManager.messages().some((message) => message.role === "assistant")).toBe(false);
  });

  test("provider-origin tool-result and tool-error events are rejected before AgentLoop", () => {
    expect(LLMEventSchema.safeParse({ type: "tool-result", id: "orphan-tool", output: { text: "done", truncated: false } }).success).toBe(false);
    expect(LLMEventSchema.safeParse({
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
    }).success).toBe(false);
  });

  test("runtime layer requests hot-state discard when invalid finish terminal append fails", async () => {
    const order: string[] = [];
    const session = new Session("sesn_1");
    const store = new AgentLoopRuntimeStore(order);
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const appended: SessionEvent[] = [];
    const writer = writerFrom((envelope) => {
      appended.push(envelope.event);
      if (envelope.event.type === "session.error" || envelope.event.type === "session.status_idle") {
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
      }
      return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
    });

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
            store,
            writer,
            events: [{ type: "finish", finishReason: "stop" }],
          }),
        ),
      ),
    );

    expect(result).toMatchObject({
      type: "failed",
      error: { type: "session-event-writer", code: "unavailable" },
      releaseSession: { reason: "event_write_failed" },
    });
    expect(appended.map((event) => event.type)).toEqual([
      "session.status_running",
      "span.model_request_start",
      "span.model_request_end",
      "session.error",
      "session.status_idle",
    ]);
    expect(appended).toContainEqual(expect.objectContaining({
      type: "span.model_request_end",
      is_error: true,
      error_kind: "runtime_semantic_error",
    }));
    expect(order).toEqual([]);
    expect(session.state.contextManager.messages().some((message) => message.role === "assistant")).toBe(false);
  });

  test("denied provider reschedule appends one exhausted error before idle", async () => {
    const order: string[] = [];
    const session = new Session("sesn_1");
    const store = new AgentLoopRuntimeStore(order);
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

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
            store,
            writer,
            events: [{ type: "provider-error", error: providerError }],
            runtimePolicy: () => ({ providerRescheduleBudget: 3, compactionRescheduleBudget: 2 }),
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
    expect(appended.at(4)).toEqual({ type: "session.status_idle", stop_reason: { type: "retries_exhausted" } });
    expect(JSON.stringify(appended)).not.toContain('"type":"retrying"');
    expect(session.state.contextManager.messages().some((message) => message.role === "assistant")).toBe(false);
  });

  test("runtime layer appends clean terminal events for provider diagnostics before durable progress", async () => {
    const order: string[] = [];
    const session = new Session("sesn_1");
    const store = new AgentLoopRuntimeStore(order);
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const appended: SessionEvent[] = [];
    const service: LLMServiceInterface = {
      stream() {
        return Stream.fail({
          type: "llm-service",
          error: runtimeFailureFromProviderError(normalizeProviderError({
            code: "provider_unavailable",
            retryable: true,
            providerId: "fake",
            modelId: "fake-chat",
            statusCode: 503,
            retryAfterMs: 7,
            message: "Provider unavailable.",
          }), { type: "terminal" }),
        } satisfies LLMServiceError);
      },
    };

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
            store,
            llmService: service,
            writer: writerFrom((envelope) => {
              appended.push(envelope.event);
              return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
            }),
          }),
        ),
      ),
    );

    expect(result).toMatchObject({
      type: "failed",
      error: {
        type: "provider",
        code: "provider_unavailable",
        message: "Provider unavailable.",
        retryable: true,
        providerId: "fake",
        modelId: "fake-chat",
        statusCode: 503,
        retryAfterMs: 7,
      },
    });
    expect(appended.at(3)).toMatchObject({
      type: "session.error",
      error: {
        code: "provider_unavailable",
        message: "Provider unavailable.",
        statusCode: 503,
        retryAfterMs: 7,
      },
    });
    expect(session.state.contextManager.messages().some((message) => message.role === "assistant")).toBe(false);
    expectNoProviderDiagnosticCanaries({ result, appended, storedMessages: [...store.messages.values()] });
  });

  test("runtime layer fails default fake cancelled provider turn without leaving streaming assistant state", async () => {
    const order: string[] = [];
    const session = new Session("sesn_1");
    const store = new AgentLoopRuntimeStore(order);
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const appended: SessionEvent[] = [];
    const service: LLMServiceInterface = {
      stream() {
        return Stream.fail({
          type: "llm-service",
          error: runtimeFailureFromProviderError(normalizeProviderError({
            code: "provider_cancelled",
            retryable: false,
            providerId: "fake",
            modelId: "fake-chat",
            message: "Provider request was cancelled.",
          }), { type: "terminal" }),
        } satisfies LLMServiceError);
      },
    };

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
            store,
            llmService: service,
            writer: writerFrom((envelope) => {
              appended.push(envelope.event);
              return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
            }),
          }),
        ),
      ),
    );

    expect(result).toMatchObject({
      type: "failed",
      error: { type: "provider", code: "provider_cancelled" },
    });
    expect(result).not.toHaveProperty("releaseSession");
    expect(appended.map((event) => event.type)).toEqual([
      "session.status_running",
      "span.model_request_start",
      "span.model_request_end",
      "session.error",
      "session.status_idle",
    ]);
    expect(appended.filter((event) => event.type === "span.model_request_end")).toEqual([
      expect.objectContaining({ type: "span.model_request_end", is_error: true, error_kind: "provider_error" }),
    ]);
    expect(appended).not.toContainEqual(expect.objectContaining({ type: "span.model_request_end", is_error: false }));
    expect(appended.at(3)).toMatchObject({ type: "session.error", error: { code: "provider_cancelled" } });
    expect(appended.at(4)).toEqual({ type: "session.status_idle", stop_reason: { type: "end_turn" } });
    expect(session.state.contextManager.messages().some((message) => message.role === "assistant")).toBe(false);
    expect(order).toEqual([]);
  });

  test("runtime layer terminalizes injected no-terminal provider progress without publishing its draft", async () => {
    const order: string[] = [];
    const session = new Session("sesn_1");
    const store = new AgentLoopRuntimeStore(order);
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const appended: SessionEvent[] = [];
    const service: LLMServiceInterface = {
      stream() {
        return Stream.fromIterable<LLMEvent>([
          { type: "text-start", id: "text-1" },
          { type: "text-delta", id: "text-1", text_delta: "visible" },
        ]);
      },
    };

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
            store,
            llmService: service,
            writer: writerFrom((envelope) => {
              appended.push(envelope.event);
              return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
            }),
          }),
        ),
      ),
    );

    expect(result).toMatchObject({
      type: "failed",
      error: { type: "runtime", code: "gateway_stream_error" },
    });
    expect(result).not.toHaveProperty("releaseSession");
    expect(appended.map((event) => event.type)).toEqual([
      "session.status_running",
      "span.model_request_start",
      "span.model_request_end",
      "session.error",
      "session.status_idle",
    ]);
    expect(appended.filter((event) => event.type === "span.model_request_end")).toEqual([
      expect.objectContaining({ type: "span.model_request_end", is_error: true, error_kind: "gateway_stream_error" }),
    ]);
    expect(appended).not.toContainEqual(expect.objectContaining({ type: "span.model_request_end", is_error: false }));
    expect(JSON.stringify(appended)).not.toContain("visible");
    expect(appended.at(3)).toMatchObject({ type: "session.error", error: { code: "gateway_stream_error" } });
    expect(session.state.contextManager.messages().some((message) => message.role === "assistant")).toBe(false);
  });

  test("runtime layer requests hot-state discard when provider-error terminal append fails before progress", async () => {
    const order: string[] = [];
    const session = new Session("sesn_1");
    const store = new AgentLoopRuntimeStore(order);
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const appendedTypes: string[] = [];
    const writer = failingEventWriter(appendedTypes, (event) => event.type === "session.error" || event.type === "session.status_idle");
    const providerError = {
      type: "provider",
      code: "provider_unavailable",
      message: "Provider unavailable.",
      retryable: true,
      fatal: false,
      retryStatus: { type: "exhausted" },
      providerId: "fake",
      modelId: "fake-chat",
    } as const;

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
            store,
            writer,
            events: [{ type: "provider-error", error: providerError }],
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
      "span.model_request_end",
      "session.error",
      "session.status_idle",
    ]);
    expect(session.state.contextManager.messages().some((message) => message.role === "assistant")).toBe(false);
  });

  test("runtime layer routes a proven terminal provider failure through atomic termination", async () => {
    const session = new Session("sesn_1");
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const appended: SessionEvent[] = [];
    const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
    const terminations: SessionEventWriterRuntimeTerminationEnvelope[] = [];
    const closeoutOrder: string[] = [];
    const baseWriter = writerFrom((envelope) => {
      appended.push(envelope.event);
      return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
    });
    const writer: SessionEventWriter = {
      ...baseWriter,
      writeRequestEnd: async (envelope) => {
        closeoutOrder.push("write_request_end");
        requestEnds.push(envelope);
        return await baseWriter.writeRequestEnd(envelope);
      },
      commitRuntimeTermination: async (envelope) => {
        closeoutOrder.push("commit_runtime_termination");
        terminations.push(envelope);
        return withRuntimeTerminationReceiptForTest(envelope, {
          ok: true,
          writeId: envelope.writeId,
          eventId: envelope.writeId,
          processedAt: createdAt,
        });
      },
    };
    const failure = {
      type: "provider",
      code: "provider_invalid_request",
      message: "Provider request is terminal.",
      retryable: false,
      fatal: true,
      retryStatus: { type: "terminal" },
      providerId: "fake",
      modelId: "fake-chat",
    } as const;

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(runtimeAgentLoopLayer(loader, {
          writer,
          events: [
            { type: "text-start", id: "terminal-text" },
            { type: "text-delta", id: "terminal-text", text_delta: "partial answer" },
            { type: "text-end", id: "terminal-text" },
            { type: "provider-error", error: failure },
          ],
        })),
      ),
    );

    expect(result).toMatchObject({ type: "failed", error: failure });
    expect(terminations).toHaveLength(1);
    expect(terminations[0]?.requestId).toBe(terminations[0]?.writeId);
    expect(terminations[0]?.writeId).toMatch(/^bridge-stid_/);
    expect(terminations[0]).toMatchObject({
      sessionId: "sesn_1",
      sessionThreadId: "thread-test",
      failure,
      drafts: [],
      pendingToolCancellations: [],
    });
    expect(closeoutOrder).toEqual(["write_request_end", "commit_runtime_termination"]);
    expect(requestEnds).toEqual([
      expect.objectContaining({
        modelRequestId: expect.any(String),
        isError: true,
        errorKind: "provider_error",
        finishReason: "error",
        drafts: [expect.objectContaining({
          sourceKind: "model_request",
          draftKind: "assistant_text",
          status: "failed",
        })],
      }),
    ]);
    expect(appended.map((event) => event.type)).toEqual([
      "session.status_running",
      "span.model_request_start",
      "agent.message",
      "span.model_request_end",
    ]);
  });

  test("runtime layer seals a terminal stream failure before atomic termination", async () => {
    const session = new Session("sesn_1");
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const closeoutOrder: string[] = [];
    const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
    const failure = {
      type: "provider",
      code: "provider_invalid_request",
      message: "Terminal stream failure.",
      retryable: false,
      fatal: true,
      retryStatus: { type: "terminal" },
      providerId: "fake",
      modelId: "fake-chat",
    } as const;
    const baseWriter = writerFrom((envelope) => ({
      ok: true,
      writeId: envelope.writeId,
      eventId: `bridge-${envelope.writeId}`,
      processedAt: createdAt,
    }));
    const writer: SessionEventWriter = {
      ...baseWriter,
      writeRequestEnd: async (envelope) => {
        closeoutOrder.push("write_request_end");
        requestEnds.push(envelope);
        return await baseWriter.writeRequestEnd(envelope);
      },
      commitRuntimeTermination: async (envelope) => {
        closeoutOrder.push("commit_runtime_termination");
        return withRuntimeTerminationReceiptForTest(envelope, {
          ok: true,
          writeId: envelope.writeId,
          eventId: envelope.writeId,
          processedAt: createdAt,
        });
      },
    };
    const service: LLMServiceInterface = {
      stream() {
        return Stream.concat(
          Stream.fromIterable<LLMEvent>([
            { type: "text-start", id: "stream-terminal-text" },
            { type: "text-delta", id: "stream-terminal-text", text_delta: "partial stream answer" },
            { type: "text-end", id: "stream-terminal-text" },
          ]),
          Stream.fail({
            type: "llm-service",
            error: failure,
          } satisfies LLMServiceError),
        );
      },
    };

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, { writer, llmService: service }))),
    );

    expect(result).toMatchObject({ type: "failed", error: failure });
    expect(closeoutOrder).toEqual(["write_request_end", "commit_runtime_termination"]);
    expect(requestEnds).toEqual([
      expect.objectContaining({
        isError: true,
        finishReason: "error",
        drafts: [expect.objectContaining({ status: "failed" })],
      }),
    ]);
    expect(session.state.contextManager.messages()).toEqual([
      expect.objectContaining({ role: "user", status: "completed" }),
      expect.objectContaining({ role: "assistant", status: "failed" }),
    ]);
  });

  test("processor settlement failure seals durable assistant content before terminal closeout", async () => {
    const session = new Session("sesn_1");
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
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

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        writer,
        events: [
          { type: "text-start", id: "processor-failure-text" },
          { type: "text-delta", id: "processor-failure-text", text_delta: "durable before repair" },
          { type: "text-end", id: "processor-failure-text" },
          { type: "step-start", stepIndex: 1 },
        ],
        createProcessor: (options) => {
          const processor = new SessionProcessor(options);
          const process = processor.process.bind(processor);
          processor.process = async (source) => {
            if (source.event.type === "step-start") {
              return {
                ok: false,
                events: [],
                error: {
                  type: "runtime",
                  code: "runtime_invalid_sequence",
                  message: "Processor settlement failed.",
                  retryable: false,
                  fatal: true,
                  reason: "runtime_contract_validation",
                },
              };
            }
            return await process(source);
          };
          return processor;
        },
      }))),
    );

    expect(result).toMatchObject({ type: "failed" });
    expect(requestEnds).toEqual([
      expect.objectContaining({
        isError: true,
        finishReason: "error",
        drafts: [expect.objectContaining({
          status: "failed",
          parts: [expect.objectContaining({ type: "text", text: "durable before repair" })],
        })],
      }),
    ]);
    expect(session.state.contextManager.messages()).toEqual([]);
  });

  test("runtime layer discards active draft before terminal provider-error events", async () => {
    const order: string[] = [];
    const session = new Session("sesn_1");
    const store = new AgentLoopRuntimeStore(order);
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const appended: SessionEvent[] = [];
    const writer = writerFrom((envelope) => {
      appended.push(envelope.event);
      return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
    });
    const providerError = {
      type: "provider",
      code: "provider_stream_error",
      message: "Stream failed.",
      retryable: false,
      fatal: true,
      providerId: "fake",
      modelId: "fake-chat",
    } as const;

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
            store,
            writer,
            events: [
              { type: "text-start", id: "text-1" },
              { type: "text-delta", id: "text-1", text_delta: "visible" },
              { type: "provider-error", error: providerError },
              { type: "text-start", id: "text-after-error" },
            ],
          }),
        ),
      ),
    );

    expect(result).toMatchObject({ type: "failed", error: providerError });
    expect(appended.map((event) => event.type)).toEqual([
      "session.status_running",
      "span.model_request_start",
      "span.model_request_end",
      "session.error",
      "session.status_idle",
    ]);
    expect(JSON.stringify(appended)).not.toContain("visible");
    expect(appended.at(3)).toEqual({
      type: "session.error",
      error: { ...providerError, retryStatus: { type: "exhausted" } },
    });
    expect(appended.at(4)).toEqual({ type: "session.status_idle", stop_reason: { type: "retries_exhausted" } });
    expect(session.state.contextManager.messages().some((message) => message.role === "assistant")).toBe(false);
    expect(order).toEqual([]);
  });

  test("terminal provider failure preserves completed text with a durable message ACK", async () => {
    const session = new Session("sesn_1");
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const appended: SessionEvent[] = [];
    const providerError = {
      type: "provider",
      code: "provider_stream_error",
      message: "Stream failed after completed text.",
      retryable: false,
      fatal: true,
      providerId: "fake",
      modelId: "fake-chat",
    } as const;

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        writer: writerFrom((envelope) => {
          appended.push(envelope.event);
          return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
        }),
        events: [
          { type: "text-start", id: "text-completed" },
          { type: "text-delta", id: "text-completed", text_delta: "durably completed" },
          { type: "text-end", id: "text-completed" },
          { type: "provider-error", error: providerError },
        ],
      }))),
    );

    expect(result).toMatchObject({ type: "failed", error: providerError });
    expect(appended.filter((event) => event.type === "agent.message")).toEqual([
      { type: "agent.message", content: [{ type: "text", text: "durably completed" }] },
    ]);
    expect(session.state.contextManager.messages().at(-1)).toMatchObject({
      role: "assistant",
      status: "failed",
      parts: [expect.objectContaining({ type: "text", text: "durably completed", status: "completed" })],
    });
  });

  test("terminal provider failure discards a tool call without a durable public tool-use identity", async () => {
    const session = new Session("sesn_1");
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const appended: SessionEvent[] = [];
    let runToolCalls = 0;
    const providerError = {
      type: "provider",
      code: "provider_stream_error",
      message: "Stream failed before tool commitment.",
      retryable: false,
      fatal: true,
      providerId: "fake",
      modelId: "fake-chat",
    } as const;

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        writer: writerFrom((envelope) => {
          appended.push(envelope.event);
          return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
        }),
        events: [
          {
            type: "tool-call",
            id: "tool-uncommitted",
            toolName: "search",
            input: { q: "x" },
            inputPreview: { value: { q: "x" }, preview: "{\"q\":\"x\"}", truncated: false },
          },
          { type: "provider-error", error: providerError },
        ],
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
          return { type: "completed", output: { text: "must not run", truncated: false } };
        },
      }))),
    );

    expect(result).toMatchObject({ type: "failed", error: providerError });
    expect(runToolCalls).toBe(0);
    expect(appended.some((event) => event.type === "agent.tool_use" || event.type === "agent.mcp_tool_use")).toBe(false);
    expect(session.state.contextManager.messages().some((message) => message.role === "assistant")).toBe(false);
  });

  test("terminal provider failure retains an atomically committed internal tool repair", async () => {
    const session = new Session("sesn_1");
    const store = new AgentLoopRuntimeStore([]);
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
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
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(Effect.provide(runtimeAgentLoopLayer(loader, {
        store,
        writer: writerFrom((envelope) => {
          if (envelope.event.type === "agent.tool_use" || envelope.event.type === "agent.tool_result") {
            publicToolEvents.push(envelope.event);
          }
          return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
        }),
        events: [
          {
            type: "tool-call",
            id: "tool-internal-repair",
            toolName: "Bash",
            input: {},
            inputPreview: { value: {}, preview: "{}", truncated: false },
          },
          { type: "provider-error", error: providerError },
        ],
        providerCallRuntime: { systemInstructions: "terminal failure internal repair test" },
        runtimePolicy: () => ({ toolCatalog: createToolCatalog({ family: "gpt" }) }),
      }))),
    );

    const repairMessage = session.state.contextManager.messages().find((message) =>
      message.status === "completed" && message.parts.some((part) => part.type === "tool" && part.state.status === "error"),
    );
    expect(result).toMatchObject({ type: "failed", error: providerError });
    expect(store.repairs).toHaveLength(1);
    expect(publicToolEvents).toEqual([]);
    expect(repairMessage).toMatchObject({
      role: "assistant",
      status: "completed",
      parts: [expect.objectContaining({ type: "tool", state: expect.objectContaining({ status: "error" }) })],
    });
    expect(repairMessage?.parts[0]).not.toHaveProperty("toolUseEventId");
  });

  test("runtime layer does not publish active draft when provider-error terminal append fails", async () => {
    const order: string[] = [];
    const session = new Session("sesn_1");
    const store = new AgentLoopRuntimeStore(order);
    const loader = new RecordingContextLoader([], { type: "messages", messages: [userMessage("user-1", 0, "hello")] });
    const appendedTypes: string[] = [];
    const writer = failingEventWriter(appendedTypes, (event) => event.type === "session.error" || event.type === "session.status_idle");
    const providerError = {
      type: "provider",
      code: "provider_stream_error",
      message: "Stream failed.",
      retryable: false,
      fatal: true,
      providerId: "fake",
      modelId: "fake-chat",
    } as const;

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agentLoop = yield* AgentLoop.Service;
        return yield* agentLoop.run(session, testRunCustody());
      }).pipe(
        Effect.provide(
          runtimeAgentLoopLayer(loader, {
            store,
            writer,
            events: [
              { type: "text-start", id: "text-1" },
              { type: "text-delta", id: "text-1", text_delta: "visible" },
              { type: "provider-error", error: providerError },
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
      "span.model_request_end",
      "session.error",
      "session.status_idle",
    ]);
    expect(session.state.contextManager.messages().some((message) => message.role === "assistant")).toBe(false);
  });
});
