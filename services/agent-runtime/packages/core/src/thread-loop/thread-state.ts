/**
 * @packageDocumentation
 * Thread-local hot ThreadState for one ThreadEntry: the in-pod working state a
 * ThreadRun mutates. Not the source of truth; recoverable from durable state.
 * SessionManager, ThreadLoop, and tool execution call it; it delegates hot message
 * list changes to ContextManager, performs ordinary in-memory transitions, and owns
 * the injected interrupt-input commit state and hot agent-mail delivery dedupe.
 *
 * OWNS (hot, per-thread):
 *   - accepted-input queue and the in-flight commit marker;
 *   - tool confirmations, pending-approval ToolJobs, task notifications, background
 *     tool handles;
 *   - runtime config patch and installed MCP manifest patches;
 *   - pending file-backed user media and tool-result transient attachments plus the
 *     single active ride;
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
 *              services/agent-runtime/packages/core/src/thread-loop/thread-loop.ts,
 *              services/agent-runtime/packages/core/src/session/session-manager.ts
 */
import { ContextManager } from "../session/context-manager.js";
import type { ThreadContextPrefix } from "../session/context-manager.js";
import type { ThreadToolRouteView, ThreadTurnCheckpoint } from "./thread-turn-checkpoint.js";
import {
  consumeThreadTurnEdge,
  deriveThreadTurnDecision,
  initializeThreadTurnReduction,
  reconcileThreadTurnSeal,
  reduceThreadTurn,
} from "./thread-turn-reducer.js";
import type {
  ThreadTurnFact,
  ThreadTurnReduction,
} from "./thread-turn-reducer.js";
import type { DurableRuntimeMessage, RuntimeDeclarationReceipt, RuntimeFailure, RuntimeJsonValue, RuntimeMessage, RuntimeMessageCreate, RuntimePart, RuntimeProcessorSource, RuntimeUsage } from "../contracts/runtime.js";
import type { RuntimeModelLimits } from "../llm/llm-event.js";
import type { ProviderRequestAttachment } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { ToolEntry } from "../tools/tool-catalog.js";
import type { ToolJob } from "../tools/tool-scheduler.js";
import type { RuntimeConfigurationPatch } from "../session/session-configuration.js";

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

export interface RuntimeControlInputDeclaration {
  readonly messageCreates: readonly RuntimeMessageCreate[];
}

export type RuntimeControlInputCommitResult =
  | { readonly ok: true; readonly stale: true }
  | { readonly ok: true; readonly joined: true }
  | { readonly ok: true; readonly receipt: RuntimeDeclarationReceipt }
  | { readonly ok: false; readonly retryable: boolean; readonly errorCode: string | number };

export type RuntimeControlInputCommit = (
  declaration: RuntimeControlInputDeclaration,
) => Promise<RuntimeControlInputCommitResult>;

export interface RuntimeControlInputCommitApplication {
  readonly declaration: RuntimeControlInputDeclaration;
  readonly result: RuntimeControlInputCommitResult;
}

interface RuntimeUserInterruptState {
  readonly command: RuntimeThreadControlState;
  readonly commitInput: RuntimeControlInputCommit;
  readonly completeCloseout: () => void;
  closeoutEligible: boolean;
  receiptApplied: boolean;
  declaration?: RuntimeControlInputDeclaration | undefined;
  commitPromise?: Promise<RuntimeControlInputCommitApplication> | undefined;
  commitResult?: RuntimeControlInputCommitResult | undefined;
}

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
  readonly parentTaskName?: string | undefined;
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

/** Fenced inter-agent delivery carrying its durably sourced user-message draft. */
export interface RuntimeInterAgentAcceptedInputState extends RuntimeThreadControlState {
  readonly kind: "inter_agent_message";
  readonly deliveryId: string;
  readonly sourceThreadId: string;
  readonly sourceToolUseEventId: string;
  readonly message: RuntimeMessage;
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

/** Bounded fact authorizing the loop to replace an undeliverable input with one rejection projection. */
export interface RuntimeRejectionAcceptedInputState extends RuntimeThreadControlState {
  readonly kind: "rejection";
  readonly reasonCode: "runtime_command_payload_too_large" | "runtime_command_rejected";
}

/** Accepted input variants queued for one thread without merging their durable identities. */
export type RuntimeAcceptedInputState =
  | RuntimeMessagesAcceptedInputState
  | RuntimeInterAgentAcceptedInputState
  | RuntimeApprovalReviewAcceptedInputState
  | RuntimeRejectionAcceptedInputState
  | RuntimeTaskNotificationAcceptedInputState;

/** Exact durable identities represented by one cold baseline. */
export interface RuntimeColdCoverage {
  readonly pendingToolIds: readonly string[];
  readonly pendingSandboxExecutionIds: readonly string[];
  readonly pendingAttachmentIdentities: readonly string[];
  readonly undeliveredMailDeliveryIds: readonly string[];
}

/** Cold thread state loaded before a resident thread is allowed to serve commands. */
export interface RuntimeThreadPreloadState extends RuntimeThreadControlState {
  readonly thread?: RuntimeAcceptedThreadMetadataState | undefined;
  readonly messages: readonly RuntimeMessage[];
  readonly turnCheckpoint?: ThreadTurnCheckpoint | undefined;
  readonly turnToolRouteView?: ThreadToolRouteView | undefined;
  readonly durableTurnId?: string | undefined;
  readonly threadContextPrefix?: ThreadContextPrefix | undefined;
  readonly runtimeBindingToken: string;
  readonly runtimeConfigPatch?: RuntimeConfigPatchState | undefined;
  readonly mcpManifests?: readonly RuntimeConfigPatchState[] | undefined;
  readonly pendingToolUses?: readonly RuntimePreloadedPendingToolUseState[] | undefined;
  readonly pendingSandboxExecutions?: readonly RuntimePreloadedSandboxExecutionState[] | undefined;
  readonly backgroundTools?: readonly RuntimePreloadedBackgroundToolState[] | undefined;
  readonly pendingAttachments?: readonly ProviderRequestAttachment[] | undefined;
  readonly pendingAgentMail?: readonly RuntimeInterAgentAcceptedInputState[] | undefined;
  readonly coldCoverage: RuntimeColdCoverage;
}

/** Durable pending tool state restored before a cold thread may resume execution. */
export interface RuntimePreloadedPendingToolUseState {
  readonly toolUseEventId: string;
  readonly modelRequestId: string;
  readonly modelToolCallId: string;
  readonly toolName: string;
  readonly input: RuntimeJsonValue;
  readonly decision?: "allow" | "deny" | undefined;
  readonly denyMessage?: string | undefined;
  readonly status: "pending" | "resolving";
}

/** Durable accepted Sandbox execution restored before its Tool Result exists. */
export interface RuntimePreloadedSandboxExecutionState {
  readonly toolUseEventId: string;
  readonly modelRequestId: string;
  readonly modelToolCallId: string;
  readonly toolName: string;
  readonly input: RuntimeJsonValue;
  readonly executionState: "pending" | "preparing" | "running" | "waiting_activation" | "waiting_materialization" | "terminal_unconsumed";
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
  readonly assistantMessage: DurableRuntimeMessage;
  readonly toolPart: Extract<RuntimePart, { readonly type: "tool" }>;
  readonly job: ToolJob;
  readonly entry: ToolEntry;
  readonly committedMessages: readonly RuntimeMessage[];
  readonly currentModel?: SessionCurrentModel | undefined;
}

/** Hot reconstruction of an accepted Sandbox execution that still needs conversation settlement. */
export interface RuntimePendingSandboxExecutionJobState {
  readonly recoveryKind: "sandbox_execution";
  readonly toolUseEventId: string;
  readonly modelRequestId: string;
  readonly source: RuntimeProcessorSource;
  readonly assistantMessage: DurableRuntimeMessage;
  readonly toolPart: Extract<RuntimePart, { readonly type: "tool" }>;
  readonly job: ToolJob;
  readonly entry: ToolEntry;
  readonly committedMessages: readonly RuntimeMessage[];
  readonly currentModel?: SessionCurrentModel | undefined;
}

/** Terminal background-task command before its durable declaration has returned a message stamp. */
export interface RuntimeTaskNotificationCommandState extends RuntimeThreadControlState {
  readonly taskId: string;
  readonly sourceToolUseEventId: string;
  readonly status: "completed" | "failed" | "cancelled" | "expired";
  readonly payloadJson: string;
}

/** Terminal task fact waiting for the next serialized semantic turn. */
export interface RuntimeTaskNotificationAcceptedInputState extends RuntimeTaskNotificationCommandState {
  readonly kind: "task_notification";
}

/** Terminal background-task fact and its receipt-stamped message installed together in hot state. */
export interface RuntimeTaskNotificationState extends RuntimeTaskNotificationCommandState {
  readonly committedMessage: DurableRuntimeMessage;
}

export interface RuntimeBackgroundToolState {
  readonly taskId: string;
  readonly sourceToolUseEventId: string;
  readonly status: "running" | "terminal";
  readonly terminalNotification?: RuntimeTaskNotificationState | undefined;
}

/** Generation-fenced runtime or per-server MCP configuration patch. */
export interface RuntimeConfigPatchState extends RuntimeThreadControlState, RuntimeConfigurationPatch {}

/** Sole resident owner of one Thread's derived current-Turn checkpoint and Tool routes. */
export class ThreadProcessor {
  #reduction: ThreadTurnReduction;
  #toolRoutes: ThreadToolRouteView;
  #acceptedInputs: RuntimeAcceptedInputState[] = [];
  #seenAgentMailByDeliveryId = Object.create(null) as Record<string, RuntimeInterAgentAcceptedInputState | undefined>;
  #committingAcceptedInputId: string | undefined;
  #acceptedInputBlockedUntilRunExit = false;

  constructor(checkpoint: ThreadTurnCheckpoint, toolRoutes: ThreadToolRouteView) {
    this.#toolRoutes = toolRoutes;
    this.#reduction = initializeThreadTurnReduction(checkpoint, toolRoutes, []);
  }

  get checkpoint(): ThreadTurnCheckpoint { return this.#reduction.checkpoint; }
  get toolRoutes(): ThreadToolRouteView { return this.#toolRoutes; }
  reduction(): ThreadTurnReduction { return this.#reduction; }

  apply(fact: ThreadTurnFact): ThreadTurnReduction {
    this.#reduction = reduceThreadTurn(this.#reduction, fact, this.#toolRoutes, this.acceptedInputIds());
    const currentToolUseEventIds = new Set(
      this.#reduction.checkpoint.request?.toolMembers.flatMap((member) =>
        member.memberKind === "public_tool_use" ? [member.toolUseEventId] : []
      ) ?? [],
    );
    this.#toolRoutes = {
      routes: this.#toolRoutes.routes.filter((route) => currentToolUseEventIds.has(route.toolUseEventId)),
    };
    return this.#reduction;
  }

  reconcileSeal(): ThreadTurnReduction {
    this.#reduction = reconcileThreadTurnSeal(this.#reduction, this.#toolRoutes, this.acceptedInputIds());
    return this.#reduction;
  }

  consumeEdge(): ThreadTurnReduction {
    this.#reduction = consumeThreadTurnEdge(this.#reduction, this.#toolRoutes, this.acceptedInputIds());
    return this.#reduction;
  }

  recordRoute(toolUseEventId: string, disposition: ThreadToolRouteView["routes"][number]["disposition"]): void {
    this.#toolRoutes = {
      routes: [
        ...this.#toolRoutes.routes.filter((route) => route.toolUseEventId !== toolUseEventId),
        { toolUseEventId, disposition },
      ],
    };
  }

  clearRoute(toolUseEventId: string): void {
    this.#toolRoutes = { routes: this.#toolRoutes.routes.filter((route) => route.toolUseEventId !== toolUseEventId) };
  }

  /**
   * Installs one accepted command into the same projection the reducer consumes.
   * Admission is the Runtime-command success boundary; durable receipt application
   * is the only operation that removes the fact or advances request readiness.
   */
  admitAcceptedInput(state: RuntimeAcceptedInputState): "applied" | "duplicate" | "conflict" {
    const existing = this.#acceptedInputs.find((input) => input.runtimeInputId === state.runtimeInputId);
    if (existing !== undefined) {
      return sameAcceptedInput(existing, state) ? "duplicate" : "conflict";
    }
    if (state.kind === "inter_agent_message") {
      const seen = this.#seenAgentMailByDeliveryId[state.deliveryId];
      if (seen !== undefined) {
        return sameAcceptedInput(seen, state) ? "duplicate" : "conflict";
      }
    }
    this.#acceptedInputs.push(state);
    if (state.kind === "inter_agent_message") {
      this.#seenAgentMailByDeliveryId[state.deliveryId] = state;
    }
    this.refreshDecision();
    return "applied";
  }

  peekAcceptedInput(): RuntimeAcceptedInputState | undefined {
    return this.#acceptedInputs[0];
  }

  acceptedInputSnapshot(): readonly RuntimeAcceptedInputState[] {
    return [...this.#acceptedInputs];
  }

  acknowledgeAcceptedInput(runtimeInputId: string): void {
    if (this.#acceptedInputs[0]?.runtimeInputId === runtimeInputId) {
      this.#acceptedInputs.shift();
    } else {
      this.#acceptedInputs = this.#acceptedInputs.filter((input) => input.runtimeInputId !== runtimeInputId);
    }
    this.refreshDecision();
  }

  discardApprovalReview(reviewId: string): void {
    this.#acceptedInputs = this.#acceptedInputs.filter(
      (input) => input.kind !== "approval_review" || input.reviewId !== reviewId,
    );
    this.refreshDecision();
  }

  beginAcceptedInputCommit(runtimeInputId: string): void {
    this.#committingAcceptedInputId = runtimeInputId;
  }

  finishAcceptedInputCommit(runtimeInputId: string): void {
    if (this.#committingAcceptedInputId === runtimeInputId) {
      this.#committingAcceptedInputId = undefined;
    }
  }

  blockAcceptedInputUntilRunExit(): void {
    this.#acceptedInputBlockedUntilRunExit = true;
    this.refreshDecision();
  }

  finishRunBoundary(): void {
    if (this.#acceptedInputBlockedUntilRunExit) {
      this.#acceptedInputBlockedUntilRunExit = false;
      this.refreshDecision();
    }
  }

  discardAcceptedInputsBeforeFence(interruptFenceSequence: number, preserveTaskNotifications: boolean): void {
    this.#acceptedInputs = this.#acceptedInputs.filter((input) =>
      input.kind === "inter_agent_message" ||
      (preserveTaskNotifications && input.kind === "task_notification") ||
      input.runtimeInputId === this.#committingAcceptedInputId ||
      input.sequenceTo >= interruptFenceSequence
    );
    this.refreshDecision();
  }

  acceptedInputCount(): number { return this.#acceptedInputs.length; }

  clearAcceptedInputs(): void {
    this.#acceptedInputs = [];
    this.#seenAgentMailByDeliveryId = Object.create(null) as Record<string, RuntimeInterAgentAcceptedInputState | undefined>;
    this.#committingAcceptedInputId = undefined;
    this.refreshDecision();
  }

  private acceptedInputIds(): readonly string[] {
    return this.#acceptedInputBlockedUntilRunExit
      ? []
      : this.#acceptedInputs.map((input) => input.runtimeInputId);
  }

  private refreshDecision(): void {
    this.#reduction = {
      ...this.#reduction,
      ...deriveThreadTurnDecision(this.#reduction.checkpoint, this.#toolRoutes, this.acceptedInputIds()),
    };
  }
}

/** Recoverable thread-local working state for input, tools, config, media, and cancellation. */
export class ThreadState {
  readonly contextManager: ContextManager;
  #persistentContextLoaded = false;
  #currentModel: SessionCurrentModel | undefined;
  #toolConfirmations: Record<string, RuntimeToolConfirmationState | undefined> = Object.create(null) as Record<
    string,
    RuntimeToolConfirmationState | undefined
  >;
  #pendingApprovalToolJobs: Record<string, RuntimePendingApprovalToolJobState | undefined> = Object.create(null) as Record<
    string,
    RuntimePendingApprovalToolJobState | undefined
  >;
  #pendingSandboxExecutionJobs: Record<string, RuntimePendingSandboxExecutionJobState | undefined> = Object.create(null) as Record<
    string,
    RuntimePendingSandboxExecutionJobState | undefined
  >;
  #taskNotifications: Record<string, RuntimeTaskNotificationState | undefined> = Object.create(null) as Record<
    string,
    RuntimeTaskNotificationState | undefined
  >;
  #backgroundTools: Record<string, RuntimeBackgroundToolState | undefined> = Object.create(null) as Record<
    string,
    RuntimeBackgroundToolState | undefined
  >;
  #activeAttachmentRide: ProviderRequestAttachment[] | undefined;
  #pendingAttachments: ProviderRequestAttachment[] = [];
  #threadProcessor: ThreadProcessor | undefined;
  #lastRequestUsage: RuntimeUsage | undefined;
  #lastRequestModelLimits: RuntimeModelLimits | undefined;
  #lastRequestContextAnchorSequence: number | undefined;
  #providerRequestOutputSchemaJson: string | undefined;
  #runtimeShutdownRequested = false;
  #cooperativeCancelRequested = false;
  #userInterrupt: RuntimeUserInterruptState | undefined;
  #lastUserInterruptCommit: { readonly runtimeInputId: string; readonly result: RuntimeControlInputCommitResult } | undefined;
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

  installThreadTurn(
    checkpoint: ThreadTurnCheckpoint | undefined,
    routeView: ThreadToolRouteView | undefined,
  ): void {
    this.#threadProcessor = new ThreadProcessor(
      checkpoint ?? { pendingInputMessageIds: [] },
      routeView ?? { routes: [] },
    );
  }

  threadTurnReduction(): ThreadTurnReduction {
    if (this.#threadProcessor === undefined) {
      this.installThreadTurn(undefined, undefined);
    }
    return this.#threadProcessor!.reduction();
  }

  applyThreadTurnFact(fact: ThreadTurnFact): ThreadTurnReduction {
    this.threadTurnReduction();
    return this.#threadProcessor!.apply(fact);
  }

  reconcileThreadTurnSeal(): ThreadTurnReduction {
    this.threadTurnReduction();
    return this.#threadProcessor!.reconcileSeal();
  }

  consumeThreadTurnEdge(): ThreadTurnReduction {
    this.threadTurnReduction();
    return this.#threadProcessor!.consumeEdge();
  }

  recordThreadToolRoute(
    toolUseEventId: string,
    disposition: ThreadToolRouteView["routes"][number]["disposition"],
  ): void {
    this.threadTurnReduction();
    this.#threadProcessor!.recordRoute(toolUseEventId, disposition);
  }

  clearThreadToolRoute(toolUseEventId: string): void {
    this.threadTurnReduction();
    this.#threadProcessor!.clearRoute(toolUseEventId);
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
    this.threadTurnReduction();
    return this.#threadProcessor!.admitAcceptedInput(state);
  }

  peekAcceptedInput(): RuntimeAcceptedInputState | undefined {
    this.threadTurnReduction();
    return this.#threadProcessor!.peekAcceptedInput();
  }

  acceptedInputSnapshot(): readonly RuntimeAcceptedInputState[] {
    this.threadTurnReduction();
    return this.#threadProcessor!.acceptedInputSnapshot();
  }

  acknowledgeAcceptedInput(runtimeInputId: string): void {
    this.threadTurnReduction();
    this.#threadProcessor!.acknowledgeAcceptedInput(runtimeInputId);
  }

  discardQueuedApprovalReview(reviewId: string): void {
    this.threadTurnReduction();
    this.#threadProcessor!.discardApprovalReview(reviewId);
  }

  beginAcceptedInputCommit(runtimeInputId: string): void {
    this.threadTurnReduction();
    this.#threadProcessor!.beginAcceptedInputCommit(runtimeInputId);
  }

  finishAcceptedInputCommit(runtimeInputId: string): void {
    this.threadTurnReduction();
    this.#threadProcessor!.finishAcceptedInputCommit(runtimeInputId);
  }

  discardQueuedAcceptedInputsBeforeFence(interruptFenceSequence: number, preserveTaskNotifications: boolean): void {
    this.threadTurnReduction();
    this.#threadProcessor!.discardAcceptedInputsBeforeFence(interruptFenceSequence, preserveTaskNotifications);
  }

  acceptedInputCount(): number {
    this.threadTurnReduction();
    return this.#threadProcessor!.acceptedInputCount();
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

  recordPendingSandboxExecutionJob(state: RuntimePendingSandboxExecutionJobState): void {
    this.#pendingSandboxExecutionJobs[state.toolUseEventId] = state;
  }

  pendingSandboxExecutionJobs(): readonly RuntimePendingSandboxExecutionJobState[] {
    return Object.values(this.#pendingSandboxExecutionJobs)
      .filter((state): state is RuntimePendingSandboxExecutionJobState => state !== undefined)
      .sort((left, right) => {
        if (left.modelRequestId !== right.modelRequestId) {
          return left.modelRequestId.localeCompare(right.modelRequestId);
        }
        return left.job.modelOrder - right.job.modelOrder;
      });
  }

  removePendingSandboxExecutionJob(toolUseEventId: string): void {
    delete this.#pendingSandboxExecutionJobs[toolUseEventId];
  }

  commitTaskNotification(state: RuntimeTaskNotificationState): "applied" | "duplicate" | "conflict" {
    const existing = this.#taskNotifications[state.taskId];
    if (existing === undefined) {
      const durableMessage = this.contextManager.message(state.committedMessage.id);
      if (
        durableMessage !== undefined &&
        JSON.stringify(durableMessage) !== JSON.stringify(state.committedMessage)
      ) {
        return "conflict";
      }
      this.#taskNotifications[state.taskId] = state;
      if (durableMessage === undefined) {
        this.contextManager.appendMessage(state.committedMessage);
      }
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
    this.addPendingAttachments(nextRide);
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

  beginRuntimeShutdown(): void {
    this.#runtimeShutdownRequested = true;
  }

  runtimeShutdownRequested(): boolean {
    return this.#runtimeShutdownRequested;
  }

  beginCooperativeCancel(): void {
    this.#cooperativeCancelRequested = true;
  }

  cooperativeCancelRequested(): boolean {
    return this.#cooperativeCancelRequested;
  }

  finishCooperativeCancel(): void {
    this.#cooperativeCancelRequested = false;
  }

  beginUserInterrupt(
    command: RuntimeThreadControlState,
    commitInput: RuntimeControlInputCommit,
    completeCloseout: () => void = () => {},
  ): "applied" | "duplicate" | "conflict" {
    if (this.#userInterrupt !== undefined) {
      return this.#userInterrupt.command.runtimeInputId === command.runtimeInputId ? "duplicate" : "conflict";
    }
    this.#userInterrupt = {
      command,
      commitInput,
      completeCloseout,
      closeoutEligible: false,
      receiptApplied: false,
    };
    this.#threadProcessor?.blockAcceptedInputUntilRunExit();
    return "applied";
  }

  finishThreadRunProjection(): void {
    this.#threadProcessor?.finishRunBoundary();
  }

  userInterruptRequested(): boolean {
    return this.#userInterrupt !== undefined;
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

  userInterruptReceiptApplied(): boolean {
    return this.#userInterrupt?.receiptApplied === true;
  }

  markUserInterruptReceiptApplied(): void {
    if (this.#userInterrupt !== undefined) {
      this.#userInterrupt.receiptApplied = true;
    }
  }

  async commitUserInterruptInput(
    declaration: RuntimeControlInputDeclaration,
  ): Promise<RuntimeControlInputCommitApplication> {
    const interrupt = this.#userInterrupt;
    if (interrupt === undefined) {
      return {
        declaration,
        result: { ok: false, retryable: true, errorCode: "interrupt_closeout_missing" },
      };
    }
    interrupt.declaration ??= declaration;
    if (interrupt.commitResult !== undefined) {
      return { declaration: interrupt.declaration, result: interrupt.commitResult };
    }
    if (interrupt.commitPromise === undefined) {
      const commitPromise = interrupt.commitInput(interrupt.declaration).then((result) => {
        if (result.ok || !result.retryable) {
          interrupt.commitResult = result;
          this.#lastUserInterruptCommit = { runtimeInputId: interrupt.command.runtimeInputId, result };
        }
        return { declaration: interrupt.declaration!, result };
      }).finally(() => {
        if (interrupt.commitPromise === commitPromise && interrupt.commitResult === undefined) {
          interrupt.commitPromise = undefined;
        }
      });
      interrupt.commitPromise = commitPromise;
    }
    return await interrupt.commitPromise;
  }

  recordJoinedUserInterruptResult(
    runtimeInputId: string,
    result: RuntimeControlInputCommitResult = { ok: true, joined: true },
    declaration: RuntimeControlInputDeclaration = {
      messageCreates: [],
    },
  ): boolean {
    const interrupt = this.#userInterrupt;
    if (interrupt?.command.runtimeInputId !== runtimeInputId) {
      return false;
    }
    interrupt.declaration = declaration;
    interrupt.commitResult = result;
    interrupt.commitPromise = Promise.resolve({ declaration, result });
    interrupt.receiptApplied = result.ok && "joined" in result;
    this.#lastUserInterruptCommit = { runtimeInputId, result };
    return true;
  }

  userInterruptCommitResult(runtimeInputId: string): RuntimeControlInputCommitResult | undefined {
    return this.#lastUserInterruptCommit?.runtimeInputId === runtimeInputId ? this.#lastUserInterruptCommit.result : undefined;
  }

  completeUserInterrupt(runtimeInputId: string): void {
    if (this.#userInterrupt?.command.runtimeInputId !== runtimeInputId) {
      return;
    }
    this.#userInterrupt.completeCloseout();
    this.#lastCompletedUserInterruptId = runtimeInputId;
    this.#userInterrupt = undefined;
  }

  userInterruptCloseoutCompleted(runtimeInputId: string): boolean {
    return this.#lastCompletedUserInterruptId === runtimeInputId;
  }

  clear(): void {
    // Generic failure cleanup cannot erase an accepted Runtime input. The
    // Session owner must first land an exact stale/close/termination receipt;
    // only that durable custody handoff may use clearAfterCustodyHandoff.
    if (this.acceptedInputCount() > 0) {
      return;
    }
    this.clearAfterCustodyHandoff();
  }

  clearAfterCustodyHandoff(): void {
    this.contextManager.clear();
    this.#persistentContextLoaded = false;
    this.#currentModel = undefined;
    this.#threadProcessor?.clearAcceptedInputs();
    this.#toolConfirmations = Object.create(null) as Record<string, RuntimeToolConfirmationState | undefined>;
    this.#pendingApprovalToolJobs = Object.create(null) as Record<string, RuntimePendingApprovalToolJobState | undefined>;
    this.#pendingSandboxExecutionJobs = Object.create(null) as Record<string, RuntimePendingSandboxExecutionJobState | undefined>;
    this.#taskNotifications = Object.create(null) as Record<string, RuntimeTaskNotificationState | undefined>;
    this.#backgroundTools = Object.create(null) as Record<string, RuntimeBackgroundToolState | undefined>;
    this.#activeAttachmentRide = undefined;
    this.#pendingAttachments = [];
    this.#threadProcessor = undefined;
    this.#lastRequestUsage = undefined;
    this.#lastRequestModelLimits = undefined;
    this.#lastRequestContextAnchorSequence = undefined;
    this.#providerRequestOutputSchemaJson = undefined;
    this.#runtimeShutdownRequested = false;
    this.#cooperativeCancelRequested = false;
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

function sameAcceptedInput(left: RuntimeAcceptedInputState, right: RuntimeAcceptedInputState): boolean {
  // Transport-attempt request ids do not change a durable input's identity.
  if (left.kind === "inter_agent_message" && right.kind === "inter_agent_message") {
    // Cold preload carries durable Thread lineage needed to construct the
    // resident aggregate; a later delivery retry does not carry that local
    // installation projection. Every mail custody and payload field must still
    // match before the delivery is considered duplicate.
    const { requestId: _leftRequestId, thread: _leftThread, ...leftIdentity } = left;
    const { requestId: _rightRequestId, thread: _rightThread, ...rightIdentity } = right;
    return JSON.stringify(leftIdentity) === JSON.stringify(rightIdentity);
  }
  return JSON.stringify({ ...left, requestId: "" }) === JSON.stringify({ ...right, requestId: "" });
}
