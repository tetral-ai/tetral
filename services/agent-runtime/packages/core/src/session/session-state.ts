/**
 * @packageDocumentation
 * Thread-local hot SessionState for one ThreadEntry: the in-pod working state a
 * ThreadRun mutates. Not the source of truth; recoverable from durable state.
 * SessionManager, AgentLoop, and tool execution call it; it delegates hot message
 * list changes to ContextManager, performs ordinary in-memory transitions, and owns
 * the injected interrupt-input commit and memoized FinishIdle closeout operations.
 *
 * OWNS (hot, per-thread):
 *   - accepted-input queue and the in-flight commit marker;
 *   - tool confirmations, pending-approval ToolJobs, task notifications, background
 *     tool handles;
 *   - runtime config patch and installed MCP manifest patches;
 *   - pending file-backed user media and tool-result transient attachments plus the
 *     single active ride;
 *   - transient model-only notes;
 *   - the last-request usage hint and held route-effective model limits;
 *   - interrupt / cooperative-cancel / runtime-shutdown controllers.
 *
 * STATE MACHINE (provider attachment ride):
 *   | state   | meaning                              | writer                      | legal transitions         |
 *   | ------- | ------------------------------------ | --------------------------- | ------------------------- |
 *   | pending | media queued for the next request    | addPendingAttachments       | pending -> riding         |
 *   | riding  | media attached to the active request | beginPendingAttachmentRide  | riding -> settled;        |
 *   |         |                                      |                             | riding retained on retry  |
 *   | settled | consumed by a settled request-end    | settlePendingAttachmentRide | terminal for that media   |
 *   Error request-ends and reschedules never settle the ride. Once an isError=false
 *   request-end acknowledges, the ride is settled even if later tool-join closeout
 *   returns an interrupted turn result. reconcilePendingAttachments preserves the
 *   unsettled active ride and separately rebuilds attachments queued for the next ride.
 *
 * INVARIANTS:
 *   - One-ride media: file-backed user media and tool-result transient attachments
 *     ride provider requests within a turn until the turn commits settled model
 *     output. Error request-ends and reschedules consume nothing: surviving media
 *     re-rides, while rejected origins remain active but stay excluded from same-
 *     turn reassembly. A successful request-end consumes the cumulative carried
 *     and rejected origin set even if later tool-join closeout reports interruption;
 *     once a settled request-end records it, later turns keep only the text projection
 *     (message-projection.ts).
 *   - The approval-reviewer model is platform runtime config: clients never choose it,
 *     Runtime Core sets it on ProviderRequest, and Gateway injects credentials but
 *     never chooses or replaces it.
 *   - Hot state is not the source of truth and stays recoverable from durable state.
 *
 * UPDATE-WITH: services/agent-runtime/packages/core/src/runtime/message-projection.ts,
 *              services/agent-runtime/packages/core/src/agent-loop/agent-loop.ts,
 *              services/agent-runtime/packages/core/src/session/session-manager.ts
 */
import { ContextManager } from "./context-manager.js";
import type { RuntimeFailure, RuntimeJsonValue, RuntimeMessage, RuntimeMessageInfo, RuntimePart, RuntimeProcessorSource, RuntimeUsage } from "../contracts/runtime.js";
import type { RuntimeModelLimits } from "../llm/llm-event.js";
import type { ProviderRequestAttachment } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { ToolEntry } from "../tools/tool-catalog.js";
import type { ToolJob } from "../tools/tool-scheduler.js";

/** Combined cap for file-backed and transient attachments on one provider request. */
export const MaxProviderRequestAttachments = 32;

/** Current provider/model selection held by a hot session thread. */
export interface SessionCurrentModel {
  readonly providerId: string;
  readonly modelId: string;
}

/** Identity and binding fence shared by every command that addresses hot thread state. */
export interface RuntimeCommandScopeState {
  readonly requestId: string;
  readonly workspaceId: string;
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly bindingId: string;
  readonly bindingGeneration: number;
  readonly targetPodUid: string;
}

/** Input identity, event range, and payload identity for one addressed thread command. */
export interface RuntimeThreadControlState extends RuntimeCommandScopeState {
  readonly runtimeInputId: string;
  readonly eventIds: readonly string[];
  readonly sequenceFrom: number;
  readonly sequenceTo: number;
}

export type RuntimeInterruptInputCommitResult =
  | { readonly ok: true }
  | { readonly ok: false; readonly retryable: boolean; readonly errorCode: string | number };

export type RuntimeInterruptInputCommit = () => Promise<RuntimeInterruptInputCommitResult>;

interface RuntimeUserInterruptState {
  readonly command: RuntimeThreadControlState;
  readonly commitInput: RuntimeInterruptInputCommit;
  readonly completeCloseout: () => void;
  closeoutEligible: boolean;
  commitPromise?: Promise<RuntimeInterruptInputCommitResult> | undefined;
  commitResult?: RuntimeInterruptInputCommitResult | undefined;
  finishIdle?: {
    readonly writeId: string;
    readonly promise: Promise<RuntimeUserInterruptFinishIdleResult>;
  } | undefined;
}

type RuntimeUserInterruptFinishIdleResult =
  | { readonly ok: true }
  | { readonly ok: false; readonly error: RuntimeFailure };

export type RuntimeThreadRoleState = "main" | "subagent" | "approval_reviewer";
export type RuntimeThreadVisibilityState = "public" | "internal";
// Runtime-side thread lifecycle. These transitions are the source Bridge commits as
// the public session.status_* and session.thread_status_* events; the runtime enum is
// wider than the public one and is projected at the durable boundary:
//   | RuntimeThreadStatusState | public projection                        |
//   | ------------------------ | ---------------------------------------- |
//   | idle                     | idle                                     |
//   | running                  | running                                  |
//   | rescheduling             | rescheduled                              |
//   | requires_action          | idle (projected at the durable boundary) |
//   | closed_for_runtime       | idle                                     |
//   | terminated               | terminated                               |
//   | failed                   | terminated                               |
// UPDATE-WITH: services/agent-runtime/packages/core/src/session/session-manager.ts,
//              services/agent-runtime/packages/core/src/runtime/session-event-writer.ts
export type RuntimeThreadStatusState =
  | "idle"
  | "running"
  | "requires_action"
  | "closed_for_runtime"
  | "rescheduling"
  | "terminated"
  | "failed";
export type RuntimeSubAgentTypeState = "general" | "research" | "worker";

/** Durable thread metadata accepted when a command first makes a thread resident. */
export interface RuntimeAcceptedThreadMetadataState {
  readonly parentThreadId?: string | undefined;
  readonly role?: RuntimeThreadRoleState | undefined;
  readonly visibility?: RuntimeThreadVisibilityState | undefined;
  readonly taskName?: string | undefined;
  readonly agentType?: RuntimeSubAgentTypeState | "approval_reviewer" | undefined;
  readonly status?: RuntimeThreadStatusState | undefined;
}

/** User-message input delivered as an opaque Bridge-classified payload. */
export interface RuntimeMessagesAcceptedInputState extends RuntimeThreadControlState {
  readonly kind: "messages";
  readonly payloadJson: string;
}

/** Fenced inter-agent delivery carrying its already-projected user message. */
export interface RuntimeInterAgentAcceptedInputState extends RuntimeThreadControlState {
  readonly kind: "inter_agent_message";
  readonly deliveryId: string;
  readonly sourceThreadId: string;
  readonly sourceToolUseEventId: string;
  readonly message: RuntimeMessage;
  readonly presentation?: "push" | "pull" | undefined;
  readonly thread?: RuntimeAcceptedThreadMetadataState | undefined;
}

/** Internal reviewer input carrying the target case, transcript feed, and output schema. */
export interface RuntimeApprovalReviewAcceptedInputState extends RuntimeThreadControlState {
  readonly kind: "approval_review";
  readonly reviewId: string;
  readonly parentThreadId: string;
  readonly targetModelToolCallId: string;
  readonly targetToolName: string;
  readonly promptItems: readonly RuntimeMessage[];
  readonly outputSchemaJson: string;
  readonly thread?: RuntimeAcceptedThreadMetadataState | undefined;
}

/** Accepted input variants queued for one thread without merging their durable identities. */
export type RuntimeAcceptedInputState =
  | RuntimeMessagesAcceptedInputState
  | RuntimeInterAgentAcceptedInputState
  | RuntimeApprovalReviewAcceptedInputState;

/** Cold thread state loaded before a resident thread is allowed to serve commands. */
export interface RuntimeThreadPreloadState extends RuntimeThreadControlState {
  readonly thread?: RuntimeAcceptedThreadMetadataState | undefined;
  readonly messages: readonly RuntimeMessage[];
  readonly runtimeBindingToken: string;
  readonly runtimeConfigPatch?: RuntimeConfigPatchState | undefined;
  readonly mcpManifests?: readonly RuntimeConfigPatchState[] | undefined;
  readonly pendingToolUses?: readonly RuntimePreloadedPendingToolUseState[] | undefined;
  readonly backgroundTools?: readonly RuntimePreloadedBackgroundToolState[] | undefined;
  readonly pendingAttachments?: readonly ProviderRequestAttachment[] | undefined;
  readonly pendingAgentMail?: readonly RuntimeInterAgentAcceptedInputState[] | undefined;
}

/** Durable pending tool state restored before a cold thread may resume execution. */
export interface RuntimePreloadedPendingToolUseState {
  readonly toolUseEventId: string;
  readonly modelRequestId: string;
  readonly modelToolCallId: string;
  readonly toolName: string;
  readonly kind: "approval" | "custom";
  readonly input: RuntimeJsonValue;
  readonly decision?: "allow" | "deny" | undefined;
  readonly denyMessage?: string | undefined;
  readonly status: "pending" | "resolving";
  readonly expiresAt: string;
}

export interface RuntimePreloadedBackgroundToolState {
  readonly taskId: string;
  readonly sourceToolUseEventId: string;
}

/** Recorded user decision applied to one durable pending tool use. */
export interface RuntimeToolConfirmationState extends RuntimeThreadControlState {
  readonly sourceEventId: string;
  readonly toolUseEventId: string;
  readonly decision: "allow" | "deny";
  readonly denyMessage?: string | undefined;
}

export interface RuntimePendingApprovalToolJobState {
  readonly toolUseEventId: string;
  readonly modelRequestId: string;
  readonly source: RuntimeProcessorSource;
  readonly assistantMessage: RuntimeMessageInfo;
  readonly toolPart: Extract<RuntimePart, { readonly type: "tool" }>;
  readonly job: ToolJob;
  readonly entry: ToolEntry;
  readonly committedMessages: readonly RuntimeMessage[];
  readonly currentModel?: SessionCurrentModel | undefined;
}

/** Terminal background-task fact and Bridge projection installed together in hot state. */
export interface RuntimeTaskNotificationState extends RuntimeThreadControlState {
  readonly taskId: string;
  readonly sourceToolUseEventId: string;
  readonly status: "completed" | "failed" | "cancelled" | "expired";
  readonly payloadJson: string;
  readonly bridgeProjection: RuntimeMessage;
}

export interface RuntimeBackgroundToolState {
  readonly taskId: string;
  readonly sourceToolUseEventId: string;
  readonly status: "running" | "terminal";
  readonly terminalNotification?: RuntimeTaskNotificationState | undefined;
}

/** Generation-fenced runtime or per-server MCP configuration patch. */
export interface RuntimeConfigPatchState extends RuntimeThreadControlState {
  readonly generation?: number;
  readonly mcpServerName?: string;
  readonly manifestETag?: string;
  readonly manifestReadiness?: "ready" | "unready";
  readonly manifestDiagnostic?: string;
  readonly payloadJson: string;
  readonly coldLoad?: true;
  readonly installedBuiltinFamily?: "claude" | "gpt";
}

/** Recoverable thread-local working state for input, tools, config, media, and cancellation. */
export class SessionState {
  readonly contextManager: ContextManager;
  #persistentContextLoaded = false;
  #currentModel: SessionCurrentModel | undefined;
  #acceptedInputs: RuntimeAcceptedInputState[] = [];
  #seenAgentMailDeliveryIds = new Set<string>();
  #interAgentMessageReceiptCommittedInRun = false;
  #committingAcceptedInputId: string | undefined;
  #toolConfirmations: Record<string, RuntimeToolConfirmationState | undefined> = Object.create(null) as Record<
    string,
    RuntimeToolConfirmationState | undefined
  >;
  #pendingApprovalToolJobs: Record<string, RuntimePendingApprovalToolJobState | undefined> = Object.create(null) as Record<
    string,
    RuntimePendingApprovalToolJobState | undefined
  >;
  #taskNotifications: Record<string, RuntimeTaskNotificationState | undefined> = Object.create(null) as Record<
    string,
    RuntimeTaskNotificationState | undefined
  >;
  #backgroundTools: Record<string, RuntimeBackgroundToolState | undefined> = Object.create(null) as Record<
    string,
    RuntimeBackgroundToolState | undefined
  >;
  #runtimeConfigPatch: RuntimeConfigPatchState | undefined;
  #coldRuntimeConfigPatch: RuntimeConfigPatchState | undefined;
  #installedBuiltinFamily: "claude" | "gpt" | undefined;
  #runtimeMcpManifestPatches: Record<string, RuntimeConfigPatchState | undefined> = Object.create(null) as Record<string, RuntimeConfigPatchState | undefined>;
  #activeAttachmentRide: ProviderRequestAttachment[] | undefined;
  #pendingAttachments: ProviderRequestAttachment[] = [];
  #pendingAttachmentOverflowCount = 0;
  #transientModelMessages: RuntimeMessage[] = [];
  #lastRequestUsage: RuntimeUsage | undefined;
  #lastRequestModelLimits: RuntimeModelLimits | undefined;
  #lastRequestContextAnchorSequence: number | undefined;
  #providerRequestOutputSchemaJson: string | undefined;
  #runtimeShutdownController = new AbortController();
  #runtimeShutdownRequested = false;
  #userInterruptController = new AbortController();
  #cooperativeCancelController = new AbortController();
  #cooperativeCancelRequested = false;
  #userInterrupt: RuntimeUserInterruptState | undefined;
  #lastUserInterruptCommit: { readonly runtimeInputId: string; readonly result: RuntimeInterruptInputCommitResult } | undefined;
  #lastCompletedUserInterruptId: string | undefined;

  constructor(sessionId: string) {
    this.contextManager = new ContextManager(sessionId);
  }

  persistentContextLoaded(): boolean {
    return this.#persistentContextLoaded;
  }

  markPersistentContextLoaded(): void {
    this.#persistentContextLoaded = true;
  }

  currentModel(): SessionCurrentModel | undefined {
    return this.#currentModel;
  }

  updateCurrentModel(model: SessionCurrentModel): void {
    if (
      this.#currentModel !== undefined &&
      (this.#currentModel.providerId !== model.providerId || this.#currentModel.modelId !== model.modelId)
    ) {
      this.clearLastRequestCompletion();
    }
    this.#currentModel = model;
  }

  enqueueAcceptedInput(state: RuntimeAcceptedInputState): "applied" | "duplicate" | "conflict" {
    if (state.kind === "inter_agent_message" && this.#seenAgentMailDeliveryIds.has(state.deliveryId)) {
      return "duplicate";
    }
    const existing = this.#acceptedInputs.find((input) => input.runtimeInputId === state.runtimeInputId);
    if (existing === undefined) {
      this.#acceptedInputs.push(state);
      if (state.kind === "inter_agent_message") {
        this.#seenAgentMailDeliveryIds.add(state.deliveryId);
      }
      return "applied";
    }
    if (sameAcceptedInput(existing, state)) {
      return "duplicate";
    }
    return "conflict";
  }

  peekAcceptedInput(): RuntimeAcceptedInputState | undefined {
    return this.#acceptedInputs[0];
  }

  acknowledgeAcceptedInput(runtimeInputId: string): void {
    if (this.#acceptedInputs[0]?.runtimeInputId === runtimeInputId) {
      this.#acceptedInputs.shift();
      return;
    }
    this.#acceptedInputs = this.#acceptedInputs.filter((input) => input.runtimeInputId !== runtimeInputId);
  }

  discardAcceptedAgentMail(deliveryId: string): void {
    this.#seenAgentMailDeliveryIds.add(deliveryId);
    this.#acceptedInputs = this.#acceptedInputs.filter(
      (input) => input.kind !== "inter_agent_message" || input.deliveryId !== deliveryId,
    );
  }

  discardQueuedApprovalReview(reviewId: string): void {
    this.#acceptedInputs = this.#acceptedInputs.filter(
      (input) => input.kind !== "approval_review" || input.reviewId !== reviewId,
    );
  }

  beginAcceptedInputCommit(runtimeInputId: string): void {
    this.#committingAcceptedInputId = runtimeInputId;
  }

  finishAcceptedInputCommit(runtimeInputId: string): void {
    if (this.#committingAcceptedInputId === runtimeInputId) {
      this.#committingAcceptedInputId = undefined;
    }
  }

  discardQueuedAcceptedInputsBeforeFence(interruptFenceSequence: number): void {
    this.#acceptedInputs = this.#acceptedInputs.filter((input) =>
      input.kind === "inter_agent_message" ||
      input.runtimeInputId === this.#committingAcceptedInputId ||
      input.sequenceTo >= interruptFenceSequence
    );
  }

  acceptedInputCount(): number {
    return this.#acceptedInputs.length;
  }

  hasQueuedInterAgentMessage(): boolean {
    return this.#acceptedInputs.some((input) => input.kind === "inter_agent_message");
  }

  markInterAgentMessageReceiptCommitted(): void {
    this.#interAgentMessageReceiptCommittedInRun = true;
  }

  takeInterAgentMessageReceiptCommitted(): boolean {
    const committed = this.#interAgentMessageReceiptCommittedInRun;
    this.#interAgentMessageReceiptCommittedInRun = false;
    return committed;
  }

  resolveToolConfirmation(state: RuntimeToolConfirmationState): "applied" | "duplicate" | "conflict" {
    const existing = this.#toolConfirmations[state.toolUseEventId];
    if (existing === undefined) {
      this.#toolConfirmations[state.toolUseEventId] = state;
      return "applied";
    }
    if (
      existing.runtimeInputId === state.runtimeInputId ||
      (existing.decision === state.decision && existing.denyMessage === state.denyMessage)
    ) {
      return "duplicate";
    }
    return "conflict";
  }

  toolConfirmation(toolUseEventId: string): RuntimeToolConfirmationState | undefined {
    return this.#toolConfirmations[toolUseEventId];
  }

  recordPendingApprovalToolJob(state: RuntimePendingApprovalToolJobState): void {
    this.#pendingApprovalToolJobs[state.toolUseEventId] = state;
  }

  pendingApprovalToolJobs(): readonly RuntimePendingApprovalToolJobState[] {
    return Object.values(this.#pendingApprovalToolJobs)
      .filter((state): state is RuntimePendingApprovalToolJobState => state !== undefined)
      .sort((left, right) => {
        if (left.modelRequestId !== right.modelRequestId) {
          return left.modelRequestId.localeCompare(right.modelRequestId);
        }
        return left.job.modelOrder - right.job.modelOrder;
      });
  }

  removePendingApprovalToolJob(toolUseEventId: string): void {
    delete this.#pendingApprovalToolJobs[toolUseEventId];
  }

  hasPendingApprovalToolJobs(): boolean {
    return Object.values(this.#pendingApprovalToolJobs).some((state) => state !== undefined);
  }

  commitTaskNotification(state: RuntimeTaskNotificationState): "applied" | "duplicate" | "conflict" {
    const existing = this.#taskNotifications[state.taskId];
    if (existing === undefined) {
      this.#taskNotifications[state.taskId] = state;
      this.contextManager.appendMessage(state.bridgeProjection);
      this.settleBackgroundTool(state);
      return "applied";
    }
    if (
      existing.runtimeInputId === state.runtimeInputId ||
      (existing.status === state.status && existing.sourceToolUseEventId === state.sourceToolUseEventId)
    ) {
      return "duplicate";
    }
    return "conflict";
  }

  taskNotification(taskId: string): RuntimeTaskNotificationState | undefined {
    return this.#taskNotifications[taskId];
  }

  recordBackgroundTool(state: Omit<RuntimeBackgroundToolState, "status" | "terminalNotification">): void {
    this.#backgroundTools[state.taskId] = {
      ...state,
      status: "running",
    };
  }

  backgroundTool(taskId: string): RuntimeBackgroundToolState | undefined {
    const state = this.#backgroundTools[taskId];
    if (state === undefined) {
      return undefined;
    }
    return {
      ...state,
      ...(state.terminalNotification !== undefined ? { terminalNotification: state.terminalNotification } : {}),
    };
  }

  private settleBackgroundTool(notification: RuntimeTaskNotificationState): void {
    const existing = this.#backgroundTools[notification.taskId];
    if (existing === undefined || existing.sourceToolUseEventId !== notification.sourceToolUseEventId) {
      return;
    }
    this.#backgroundTools[notification.taskId] = {
      taskId: notification.taskId,
      sourceToolUseEventId: notification.sourceToolUseEventId,
      status: "terminal",
      terminalNotification: notification,
    };
  }

  addPendingAttachments(attachments: readonly ProviderRequestAttachment[]): void {
    const available = Math.max(0, MaxProviderRequestAttachments - this.#pendingAttachments.length);
    this.#pendingAttachments.push(...attachments.slice(0, available).map(cloneProviderRequestAttachment));
    this.#pendingAttachmentOverflowCount += Math.max(0, attachments.length - available);
  }

  pendingAttachments(): readonly ProviderRequestAttachment[] {
    return [
      ...(this.#activeAttachmentRide ?? []),
      ...this.#pendingAttachments,
    ].map(cloneProviderRequestAttachment);
  }

  beginPendingAttachmentRide(): readonly ProviderRequestAttachment[] {
    if (this.#activeAttachmentRide === undefined && this.#pendingAttachments.length > 0) {
      this.#activeAttachmentRide = this.#pendingAttachments;
      this.#pendingAttachments = [];
    }
    return (this.#activeAttachmentRide ?? []).map(cloneProviderRequestAttachment);
  }

  settlePendingAttachmentRide(): void {
    this.#activeAttachmentRide = undefined;
  }

  replacePendingAttachments(attachments: readonly ProviderRequestAttachment[]): void {
    this.#activeAttachmentRide = undefined;
    this.#pendingAttachments = [];
    this.#pendingAttachmentOverflowCount = 0;
    this.addPendingAttachments(attachments);
  }

  reconcilePendingAttachments(attachments: readonly ProviderRequestAttachment[]): void {
    if (this.#activeAttachmentRide === undefined) {
      this.replacePendingAttachments(attachments);
      return;
    }
    const activeByOrigin = Object.create(null) as Record<string, number | undefined>;
    for (const attachment of this.#activeAttachmentRide) {
      const identity = providerRequestAttachmentIdentity(attachment);
      activeByOrigin[identity] = (activeByOrigin[identity] ?? 0) + 1;
    }
    const nextRide = attachments.filter((attachment) => {
      const identity = providerRequestAttachmentIdentity(attachment);
      const remaining = activeByOrigin[identity] ?? 0;
      if (remaining === 0) {
        return true;
      }
      activeByOrigin[identity] = remaining - 1;
      return false;
    });
    this.#pendingAttachments = [];
    this.#pendingAttachmentOverflowCount = 0;
    this.addPendingAttachments(nextRide);
  }

  takePendingAttachmentOverflowCount(): number {
    if (this.#activeAttachmentRide !== undefined) {
      return 0;
    }
    const count = this.#pendingAttachmentOverflowCount;
    this.#pendingAttachmentOverflowCount = 0;
    return count;
  }

  addTransientModelMessage(message: RuntimeMessage): void {
    this.#transientModelMessages.push(cloneRuntimeMessage(message));
  }

  transientModelMessages(): readonly RuntimeMessage[] {
    return this.#transientModelMessages.map(cloneRuntimeMessage);
  }

  transientModelMessageCount(): number {
    return this.#transientModelMessages.length;
  }

  consumeTransientModelMessages(messages: readonly RuntimeMessage[]): void {
    const consumedIds = new Set(messages.map((message) => message.id));
    this.#transientModelMessages = this.#transientModelMessages.filter((message) => !consumedIds.has(message.id));
  }

  recordLastRequestCompletion(usage: RuntimeUsage, limits: RuntimeModelLimits, contextAnchorSequence: number): void {
    this.#lastRequestUsage = { ...usage };
    this.#lastRequestModelLimits = { ...limits };
    this.#lastRequestContextAnchorSequence = contextAnchorSequence;
  }

  lastRequestUsage(): RuntimeUsage | undefined {
    return this.#lastRequestUsage === undefined ? undefined : { ...this.#lastRequestUsage };
  }

  lastRequestModelLimits(): RuntimeModelLimits | undefined {
    return this.#lastRequestModelLimits === undefined ? undefined : { ...this.#lastRequestModelLimits };
  }

  lastRequestContextAnchorSequence(): number | undefined {
    return this.#lastRequestContextAnchorSequence;
  }

  clearLastRequestUsage(): void {
    this.#lastRequestUsage = undefined;
    this.#lastRequestContextAnchorSequence = undefined;
  }

  clearLastRequestCompletion(): void {
    this.#lastRequestUsage = undefined;
    this.#lastRequestModelLimits = undefined;
    this.#lastRequestContextAnchorSequence = undefined;
  }

  setProviderRequestOutputSchemaJson(outputSchemaJson: string | undefined): void {
    this.#providerRequestOutputSchemaJson = outputSchemaJson;
  }

  providerRequestOutputSchemaJson(): string | undefined {
    return this.#providerRequestOutputSchemaJson;
  }

  applyRuntimeConfigPatch(state: RuntimeConfigPatchState): "applied" | "stale" {
    if (state.mcpServerName !== undefined || state.manifestETag !== undefined || state.manifestReadiness !== undefined) {
      if (state.mcpServerName === undefined || state.generation === undefined || state.generation <= 0 ||
        (state.manifestReadiness !== "unready" && state.manifestETag === undefined)) {
        return "stale";
      }
      const existing = this.#runtimeMcpManifestPatches[state.mcpServerName];
      if (existing?.generation !== undefined && state.generation <= existing.generation) {
        return "stale";
      }
      this.#runtimeMcpManifestPatches[state.mcpServerName] = state;
      return "applied";
    }
    if (state.generation === undefined) {
      return "stale";
    }
    if (state.coldLoad === true) {
      if (this.#coldRuntimeConfigPatch !== undefined) {
        return "stale";
      }
      this.#coldRuntimeConfigPatch = state;
      this.#installedBuiltinFamily = state.installedBuiltinFamily;
    }
    if (this.#runtimeConfigPatch?.generation !== undefined && state.generation <= this.#runtimeConfigPatch.generation) {
      return "stale";
    }
    this.#runtimeConfigPatch = state;
    return "applied";
  }

  runtimeConfigPatch(): RuntimeConfigPatchState | undefined {
    return this.#runtimeConfigPatch;
  }

  runtimeConfigPatches(): readonly RuntimeConfigPatchState[] {
    return [
      ...(this.#coldRuntimeConfigPatch !== undefined ? [this.#coldRuntimeConfigPatch] : []),
      ...(this.#runtimeConfigPatch !== undefined && this.#runtimeConfigPatch !== this.#coldRuntimeConfigPatch
        ? [this.#runtimeConfigPatch]
        : []),
      ...Object.values(this.#runtimeMcpManifestPatches).filter((value): value is RuntimeConfigPatchState => value !== undefined),
    ];
  }

  installedBuiltinFamily(): "claude" | "gpt" | undefined {
    return this.#installedBuiltinFamily;
  }

  beginRuntimeShutdown(): AbortSignal {
    this.#runtimeShutdownRequested = true;
    if (!this.#runtimeShutdownController.signal.aborted) {
      this.#runtimeShutdownController.abort();
    }
    return this.#runtimeShutdownController.signal;
  }

  runtimeShutdownSignal(): AbortSignal {
    return this.#runtimeShutdownController.signal;
  }

  runtimeShutdownRequested(): boolean {
    return this.#runtimeShutdownRequested;
  }

  beginCooperativeCancel(): AbortSignal {
    this.#cooperativeCancelRequested = true;
    if (!this.#cooperativeCancelController.signal.aborted) {
      this.#cooperativeCancelController.abort();
    }
    return this.#cooperativeCancelController.signal;
  }

  cooperativeCancelSignal(): AbortSignal {
    return this.#cooperativeCancelController.signal;
  }

  cooperativeCancelRequested(): boolean {
    return this.#cooperativeCancelRequested;
  }

  finishCooperativeCancel(): void {
    this.#cooperativeCancelRequested = false;
    this.#cooperativeCancelController = new AbortController();
  }

  beginUserInterrupt(
    command: RuntimeThreadControlState,
    commitInput: RuntimeInterruptInputCommit,
    completeCloseout: () => void = () => {},
  ): "applied" | "duplicate" | "conflict" {
    if (this.#userInterrupt !== undefined) {
      return this.#userInterrupt.command.runtimeInputId === command.runtimeInputId ? "duplicate" : "conflict";
    }
    this.#userInterrupt = { command, commitInput, completeCloseout, closeoutEligible: false };
    this.#userInterruptController.abort();
    return "applied";
  }

  userInterruptRequested(): boolean {
    return this.#userInterrupt !== undefined;
  }

  userInterruptSignal(): AbortSignal {
    return this.#userInterruptController.signal;
  }

  userInterruptCommand(): RuntimeThreadControlState | undefined {
    return this.#userInterrupt?.command;
  }

  markUserInterruptCloseoutEligible(): void {
    if (this.#userInterrupt !== undefined) {
      this.#userInterrupt.closeoutEligible = true;
    }
  }

  userInterruptCloseoutEligible(): boolean {
    return this.#userInterrupt?.closeoutEligible === true;
  }

  async commitUserInterruptInput(): Promise<RuntimeInterruptInputCommitResult> {
    const interrupt = this.#userInterrupt;
    if (interrupt === undefined) {
      return { ok: false, retryable: true, errorCode: "interrupt_closeout_missing" };
    }
    interrupt.commitPromise ??= interrupt.commitInput().then((result) => {
      interrupt.commitResult = result;
      this.#lastUserInterruptCommit = { runtimeInputId: interrupt.command.runtimeInputId, result };
      return result;
    });
    return await interrupt.commitPromise;
  }

  userInterruptCommitResult(runtimeInputId: string): RuntimeInterruptInputCommitResult | undefined {
    return this.#lastUserInterruptCommit?.runtimeInputId === runtimeInputId ? this.#lastUserInterruptCommit.result : undefined;
  }

  async joinOrStartUserInterruptFinishIdle(
    runtimeInputId: string,
    prepare: () => { readonly writeId: string; readonly run: () => Promise<RuntimeUserInterruptFinishIdleResult> },
  ): Promise<RuntimeUserInterruptFinishIdleResult | undefined> {
    const interrupt = this.#userInterrupt;
    if (interrupt?.command.runtimeInputId !== runtimeInputId) {
      return undefined;
    }
    if (interrupt.finishIdle === undefined) {
      const prepared = prepare();
      const promise = Promise.resolve().then(prepared.run);
      interrupt.finishIdle = { writeId: prepared.writeId, promise };
    }
    return await interrupt.finishIdle.promise;
  }

  completeUserInterrupt(runtimeInputId: string): void {
    if (this.#userInterrupt?.command.runtimeInputId !== runtimeInputId) {
      return;
    }
    this.#userInterrupt.completeCloseout();
    this.#lastCompletedUserInterruptId = runtimeInputId;
    this.#userInterrupt = undefined;
    this.#userInterruptController = new AbortController();
  }

  userInterruptCloseoutCompleted(runtimeInputId: string): boolean {
    return this.#lastCompletedUserInterruptId === runtimeInputId;
  }

  clear(): void {
    this.contextManager.clear();
    this.#persistentContextLoaded = false;
    this.#currentModel = undefined;
    this.#acceptedInputs = [];
    this.#seenAgentMailDeliveryIds.clear();
    this.#interAgentMessageReceiptCommittedInRun = false;
    this.#committingAcceptedInputId = undefined;
    this.#toolConfirmations = Object.create(null) as Record<string, RuntimeToolConfirmationState | undefined>;
    this.#pendingApprovalToolJobs = Object.create(null) as Record<string, RuntimePendingApprovalToolJobState | undefined>;
    this.#taskNotifications = Object.create(null) as Record<string, RuntimeTaskNotificationState | undefined>;
    this.#backgroundTools = Object.create(null) as Record<string, RuntimeBackgroundToolState | undefined>;
    this.#runtimeConfigPatch = undefined;
    this.#coldRuntimeConfigPatch = undefined;
    this.#installedBuiltinFamily = undefined;
    this.#runtimeMcpManifestPatches = Object.create(null) as Record<string, RuntimeConfigPatchState | undefined>;
    this.#activeAttachmentRide = undefined;
    this.#pendingAttachments = [];
    this.#pendingAttachmentOverflowCount = 0;
    this.#transientModelMessages = [];
    this.#lastRequestUsage = undefined;
    this.#lastRequestModelLimits = undefined;
    this.#lastRequestContextAnchorSequence = undefined;
    this.#providerRequestOutputSchemaJson = undefined;
    this.#runtimeShutdownController = new AbortController();
    this.#runtimeShutdownRequested = false;
    this.#userInterruptController = new AbortController();
    this.#userInterrupt = undefined;
    this.#lastUserInterruptCommit = undefined;
    this.#lastCompletedUserInterruptId = undefined;
  }
}

function cloneProviderRequestAttachment(attachment: ProviderRequestAttachment): ProviderRequestAttachment {
  return {
    ...attachment,
    transient: attachment.transient === undefined ? undefined : { ...attachment.transient },
    fileBacked: attachment.fileBacked === undefined ? undefined : { ...attachment.fileBacked },
  };
}

function providerRequestAttachmentIdentity(attachment: ProviderRequestAttachment): string {
  if (attachment.transient !== undefined) {
    return JSON.stringify(["transient", attachment.transient.attachmentRef]);
  }
  if (attachment.fileBacked !== undefined) {
    return JSON.stringify(["file", attachment.fileBacked.sourceEventId, attachment.fileBacked.fileId]);
  }
  return JSON.stringify(["invalid", attachment]);
}

function cloneRuntimeMessage(message: RuntimeMessage): RuntimeMessage {
  return {
    ...message,
    parts: message.parts.map((part) => ({ ...part })),
  };
}

function sameAcceptedInput(left: RuntimeAcceptedInputState, right: RuntimeAcceptedInputState): boolean {
  return JSON.stringify({ ...left, requestId: "" }) === JSON.stringify({ ...right, requestId: "" });
}
