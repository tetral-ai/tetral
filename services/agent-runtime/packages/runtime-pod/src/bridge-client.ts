/**
 * @packageDocumentation
 * Adapts Runtime Core persistence and lifecycle ports to authenticated Agent Runtime Bridge
 * gRPC calls. Runtime Pod composition constructs these adapters, and Runtime Core uses them to
 * load cold thread state, commit inputs and events, manage reviewer threads, repair internal tool
 * state, and refresh binding tokens. The adapters call the generated Bridge client and outbound
 * metadata factory, accept committed or duplicate acknowledgements as durable proof, validate
 * populated projected JSON before exposing it, and keep per-thread scope bookkeeping local and disposable.
 */
import { createHash } from "node:crypto";
import { credentials, status } from "@grpc/grpc-js";
import type { CallOptions, Metadata, ServiceError } from "@grpc/grpc-js";
import {
  AgentRuntimeBridgeServiceClient,
  BridgeWriteStatus,
  ChildLifecycleDisposition,
  DurableEventDisposition,
  DurableProjectionDisposition,
  PrefixConsumptionDisposition,
  ReceiptApplicationDisposition,
  RequestRescheduleDisposition,
  RuntimeInputDisposition,
  RuntimeMessageCreateKind,
  WriteEventRequest as WriteEventRequestMessage,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type {
  CommitInputsRequest,
  CommitInputsResponse,
  DeclarationReceipt as BridgeDeclarationReceipt,
  CommitInternalToolRepairRequest,
  CommitInternalToolRepairResponse,
  CommitRuntimeTerminationRequest,
  CommitRuntimeTerminationResponse,
  CommitTaskNotificationResultResponse,
  AdmitApprovalReviewInputRequest,
  AdmitApprovalReviewInputResponse,
  CloseApprovalReviewerRequest,
  CloseApprovalReviewerResponse,
  EnsureApprovalReviewerSidecarRequest,
  EnsureApprovalReviewerSidecarResponse,
  EnsureApprovalReviewerTrunkRequest,
  EnsureApprovalReviewerTrunkResponse,
  FinishIdleRequest,
  FinishIdleResponse,
  LoadContextRequest,
  LoadContextResponse,
  MarkChildThreadActiveResponse,
  ReadAgentMailRequest,
  ReadAgentMailResponse,
  RefreshRuntimeBindingTokenRequest,
  RefreshRuntimeBindingTokenResponse,
  RuntimeScope,
  RuntimeInterruptToolResult,
  SettleToolResultRequest,
  SettleToolResultResponse,
  WriteEventRequest,
  WriteEventResponse,
  WriteRequestEndRequest,
  WriteRequestEndResponse,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type { AcceptedInputCommitResult, ContextLoader, RuntimeLoadedAgentMail, RuntimeLoadedPendingToolUse } from "@tetral/agent-runtime-core/src/context/context-loader.js";
import type {
  RuntimeAcceptedInputState,
  RuntimeAcceptedThreadMetadataState,
  RuntimeConfigPatchState,
  RuntimePreloadedBackgroundToolState,
  RuntimePreloadedSandboxExecutionState,
  RuntimeThreadAddressState,
  RuntimeColdCoverage,
} from "@tetral/agent-runtime-core/src/thread-loop/thread-state.js";
import type { RuntimeThreadIdentity } from "@tetral/agent-runtime-core/src/thread-loop/thread-runtime.js";
import type { ThreadContextPrefix } from "@tetral/agent-runtime-core/src/session/context-manager.js";
import { ThreadTurnLoadFactsSchema } from "@tetral/agent-runtime-core/src/thread-loop/thread-turn-checkpoint.js";
import type { ThreadTurnLoadFacts } from "@tetral/agent-runtime-core/src/thread-loop/thread-turn-checkpoint.js";
import {
  DurableRuntimeMessageSchema,
  RuntimeMessageSchema,
  RuntimeJsonValueSchema,
  RuntimeToolErrorSchema,
  SessionEventWriterRetryPolicy,
  normalizeContextLoaderError,
  normalizeRuntimeMessageStoreError,
  normalizeSessionEventWriterError,
  runtimeToolErrorFromFailure,
} from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import { MailFetchMaxEnvelopes } from "@tetral/agent-runtime-protocol/src/bounds.js";
import { runtimeMessageFromAgentMailContent } from "./agent-mail.js";
import type {
  DurableRuntimeMessage,
  RuntimeProviderAttachment,
  RuntimeMessageCreate as CoreRuntimeMessageCreate,
  RuntimePartCreate as CoreRuntimePartCreate,
  RuntimeToolSettlementDeclaration,
  RuntimeInternalToolRepairCommit,
  RuntimeInternalToolRepairCommitResult,
  RuntimeMessage,
  SessionEventEnvelope,
  SessionEventWriter,
  SessionEventWriterAppendResult,
  SessionEventWriterFinishIdleEnvelope,
  SessionEventWriterRequestEndEnvelope,
  SessionEventWriterRuntimeTerminationEnvelope,
  SessionEventWriterRuntimeTerminationResult,
  SessionEventWriterToolSettlementEnvelope,
  SessionEventWriterToolSettlementAttempt,
} from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import {
  acceptedInputDeclarationKind,
  applyTaskNotificationReceipt,
  taskNotificationOperationId,
} from "@tetral/agent-runtime-core/src/runtime/runtime-declaration.js";
import type { RuntimeDeclarationReceipt } from "@tetral/agent-runtime-core/src/runtime/runtime-declaration.js";
import { buildOutboundBearerMetadata } from "./auth.js";
import type { ServiceAccountTokenConfig } from "./auth.js";
import type { ApprovalReviewerThreadCreation, RuntimeApprovalReviewerThreadCreator } from "./approval-reviewer.js";
import type { RuntimeControlInputCommitter } from "./runtime-service.js";
import {
  recordContextLoadParseFailure,
  recordRuntimeReceiptEvidence,
} from "./logger.js";
import type {
  RuntimeContextLoadParsePhase,
  RuntimeContextLoadParseReason,
  RuntimePodLogger,
  RuntimeReceiptEvidence,
  RuntimeReceiptEvidenceMetrics,
} from "./logger.js";
import {
  MaxBridgeDurableContextGrpcMessageBytes,
  bridgeDurableContextGrpcChannelOptions,
  grpcClientChannelOptions,
} from "./bounds.js";
import { sessionEventForDurableWrite } from "@tetral/agent-runtime-core/src/runtime/session-event-writer.js";
import {
  commitInputsDeclarationDigest,
  finishIdleDeclarationDigest,
  internalToolRepairDeclarationDigest,
  taskNotificationDeclarationDigest,
  writeEventDeclarationDigest,
  writeRequestEndDeclarationDigest,
} from "./runtime-declaration-wire.js";

interface BridgeDeclarationEvidenceOptions {
  readonly logger?: RuntimePodLogger | undefined;
  readonly metrics?: RuntimeReceiptEvidenceMetrics | undefined;
}

function recordBridgeReceiptEvidence(
  options: BridgeDeclarationEvidenceOptions,
  evidence: RuntimeReceiptEvidence,
): void {
  recordRuntimeReceiptEvidence(options.logger, options.metrics, evidence);
}

/** Configures the Bridge adapter that durably settles interrupt and tool-confirmation inputs. */
export interface BridgeAPIControlInputCommitterOptions extends BridgeDeclarationEvidenceOptions {
  readonly address: string;
  readonly tokenPath: string;
  readonly metadataFactory?: (config: ServiceAccountTokenConfig) => Promise<Metadata>;
  readonly client?: AgentRuntimeBridgeServiceClient;
  readonly sleep?: (durationMs: number) => Promise<void>;
}

/**
 * Commits control inputs through Bridge before Runtime Core treats the control action as durable.
 * Authentication and transport failures remain retryable unless the gRPC status identifies a
 * deterministic request or idempotency conflict; only committed or duplicate ACKs succeed.
 */
export class BridgeAPIControlInputCommitter implements RuntimeControlInputCommitter {
  private readonly client: AgentRuntimeBridgeServiceClient;
  private readonly metadataFactory: (config: ServiceAccountTokenConfig) => Promise<Metadata>;
  private readonly sleep: (durationMs: number) => Promise<void>;

  constructor(private readonly options: BridgeAPIControlInputCommitterOptions) {
    this.client = options.client ?? new AgentRuntimeBridgeServiceClient(options.address, credentials.createInsecure(), grpcClientChannelOptions());
    this.metadataFactory = options.metadataFactory ?? buildOutboundBearerMetadata;
    this.sleep = options.sleep ?? (async (durationMs) => await new Promise<void>((resolve) => setTimeout(resolve, durationMs)));
  }

  /** Commits one frozen interrupt or tool-confirmation declaration. */
  async commitControlInput(input: Parameters<RuntimeControlInputCommitter["commitControlInput"]>[0]) {
    let metadata: Metadata;
    try {
      metadata = await this.metadataFactory({ tokenPath: this.options.tokenPath });
    } catch {
      return {
        ok: false as const,
        retryable: true,
        errorCode: "bridge_token_unavailable",
        message: "control input durable commit failed",
      };
    }
    const request: CommitInputsRequest = {
      scope: bridgeScope(input.scope),
      runtimeInputId: input.scope.runtimeInputId,
      disposition: RuntimeInputDisposition.RUNTIME_INPUT_DISPOSITION_COMMIT,
      approvalReviewText: [],
    };
    const declarationDigest = commitInputsDeclarationDigest(request, input.inputKind);
    let response: CommitInputsResponse | undefined;
    for (let attempt = 0; response === undefined; attempt += 1) {
      try {
        response = await commitInputs(this.client, request, metadata);
      } catch (error) {
        if (
          !bridgeDeclarationTransportUnknown(error) ||
          attempt >= SessionEventWriterRetryPolicy.backoffMs.length
        ) {
          return {
            ok: false as const,
            retryable: bridgeCommitErrorRetryable(error),
            errorCode: "bridge_commit_unavailable",
            message: "control input durable commit failed",
          };
        }
        const backoffMs = SessionEventWriterRetryPolicy.backoffMs[attempt]!;
        await this.sleep(backoffMs);
      }
    }
    if (!exactlyOneDefined(response.committed, response.duplicate, response.stale)) {
      return {
        ok: false as const,
        retryable: false,
        errorCode: "bridge_commit_rejected",
        message: "control input durable commit returned malformed outcome",
      };
    }
    if (response.stale !== undefined) {
      return { ok: true as const, stale: true as const };
    }
    const result = response.committed ?? response.duplicate;
    if (result !== undefined) {
      if (!exactlyOneDefined(result.context, result.interrupt)) {
        return {
          ok: false as const,
          retryable: false,
          errorCode: "bridge_commit_rejected",
          message: "control input durable commit returned malformed application",
        };
      }
      if (input.inputKind === "interrupt_control" && result.interrupt !== undefined) {
        try {
          const receipt = hotReceiptFromAssignedContext({
            sessionId: input.scope.sessionId,
            sessionThreadId: input.scope.sessionThreadId,
            sourceKind: input.inputKind,
            runtimeInputId: input.scope.runtimeInputId,
            declarationDigest,
            creates: input.messageCreates,
            assignedContextSequences: [],
            interruptToolResults: result.interrupt.interruptToolResults,
            pendingAttachmentJson: [],
          });
          return { ok: true as const, receipt };
        } catch {
          return {
            ok: false as const,
            retryable: false,
            errorCode: "bridge_commit_rejected",
            message: "control input durable commit returned malformed interrupt application",
          };
        }
      }
      if (input.inputKind !== "interrupt_control" && result.context !== undefined) {
        try {
          const receipt = hotReceiptFromAssignedContext({
            sessionId: input.scope.sessionId,
            sessionThreadId: input.scope.sessionThreadId,
            sourceKind: input.inputKind,
            runtimeInputId: input.scope.runtimeInputId,
            declarationDigest,
            creates: input.messageCreates,
            assignedContextSequences: result.context.assignedContextSequences,
            interruptToolResults: [],
            pendingAttachmentJson: result.context.pendingAttachmentJson,
          });
          return { ok: true as const, receipt };
        } catch {
          return {
            ok: false as const,
            retryable: false,
            errorCode: "bridge_commit_rejected",
            message: "control input durable commit returned malformed context application",
          };
        }
      }
      return {
        ok: false as const,
        retryable: false,
        errorCode: "bridge_commit_rejected",
        message: "control input durable commit returned mismatched application",
      };
    }
    return {
      ok: false as const,
      retryable: false,
      errorCode: "bridge_commit_rejected",
      message: "control input durable commit rejected",
    };
  }
}

/** Configures the Bridge adapter for dedicated background-task notification settlement. */
export interface BridgeAPITaskNotificationCommitterOptions extends BridgeDeclarationEvidenceOptions {
  readonly address: string;
  readonly tokenPath: string;
  readonly metadataFactory?: (config: ServiceAccountTokenConfig) => Promise<Metadata>;
  readonly client?: AgentRuntimeBridgeServiceClient;
}

/**
 * Settles a loop-authored terminal background-task notification through Bridge and applies the
 * returned durable stamps before handing the message to Runtime Core. A stale Bridge disposition
 * is an idempotent success, while malformed receipts fail without retry.
 */
export class BridgeAPITaskNotificationCommitter {
  private readonly client: AgentRuntimeBridgeServiceClient;
  private readonly metadataFactory: (config: ServiceAccountTokenConfig) => Promise<Metadata>;

  constructor(private readonly options: BridgeAPITaskNotificationCommitterOptions) {
    this.client = options.client ?? new AgentRuntimeBridgeServiceClient(options.address, credentials.createInsecure(), grpcClientChannelOptions());
    this.metadataFactory = options.metadataFactory ?? buildOutboundBearerMetadata;
  }

  /** Commits one task-notification draft and returns its receipt-stamped message or stale disposition. */
  async commitTaskNotification(input: {
    readonly scope: RuntimeAcceptedInputState;
    readonly command: {
      readonly runtimeInputId: string;
      readonly taskId: string;
      readonly sourceToolUseEventId: string;
      readonly status: "completed" | "failed" | "cancelled" | "expired";
      readonly payloadJson: string;
      readonly messageCreate: CoreRuntimeMessageCreate;
    };
  }) {
    let metadata: Metadata;
    try {
      metadata = await this.metadataFactory({ tokenPath: this.options.tokenPath });
    } catch {
      return {
        ok: false as const,
        retryable: true,
        errorCode: "bridge_token_unavailable",
        message: "task notification durable commit failed",
      };
    }
    const request = {
      scope: {
        workspaceId: input.scope.workspaceId,
        sessionId: input.scope.sessionId,
        sessionThreadId: input.scope.sessionThreadId,
        binding: {
          bindingId: input.scope.bindingId,
          bindingGeneration: input.scope.bindingGeneration,
          targetPodUid: input.scope.targetPodUid,
        },
      },
      runtimeInputId: input.command.runtimeInputId,
      disposition: RuntimeInputDisposition.RUNTIME_INPUT_DISPOSITION_COMMIT,
    };
    const declarationDigest = taskNotificationDeclarationDigest(request);
    let response: CommitTaskNotificationResultResponse;
    try {
      response = await commitTaskNotificationResult(this.client, request, metadata);
    } catch (error) {
      const code = grpcStatusCode(error);
      if (code === status.INVALID_ARGUMENT) {
        return {
          ok: false as const,
          retryable: false,
          errorCode: "task_notification_result_invalid",
          message: "task notification durable commit failed",
        };
      }
      if (code === status.ALREADY_EXISTS) {
        return {
          ok: true as const,
          rejected: true as const,
          errorCode: "task_notification_payload_mismatch" as const,
        };
      }
      return {
        ok: false as const,
        // A queued Inbox without Queue custody is a durable invariant violation;
        // live-lease contention and all other transport failures remain retryable.
        retryable: code !== status.FAILED_PRECONDITION,
        errorCode: "bridge_commit_unavailable",
        message: "task notification durable commit failed",
      };
    }
    if (!exactlyOneDefined(
      response.committed,
      response.duplicate,
      response.stale,
      response.deferred,
      response.rejected,
    )) {
      return {
        ok: false as const,
        retryable: true,
        errorCode: "bridge_task_notification_projection_invalid",
        message: "task notification durable commit returned malformed outcome",
      };
    }
    const committed = response.committed ?? response.duplicate;
    if (committed !== undefined) {
      const operationId = taskNotificationOperationId(input.command.runtimeInputId, input.command.taskId);
      try {
        const receipt = hotTaskNotificationReceiptFromAssignedContext({
          sessionId: input.scope.sessionId,
          sessionThreadId: input.scope.sessionThreadId,
          runtimeInputId: input.command.runtimeInputId,
          taskId: input.command.taskId,
          declarationDigest,
          create: input.command.messageCreate,
          assignedContextSequences: committed.assignedContextSequences,
        });
        const message = applyTaskNotificationReceipt({
          sessionId: input.scope.sessionId,
          sessionThreadId: input.scope.sessionThreadId,
          operationId,
          create: input.command.messageCreate,
        }, receipt);
        return {
          ok: true as const,
          committedMessage: message,
          receipt,
          inputDisposition: response.duplicate !== undefined
            ? "duplicate" as const
            : "committed" as const,
        };
      } catch {
        return {
          ok: false as const,
          retryable: true,
          errorCode: "bridge_task_notification_projection_invalid",
          message: "task notification durable commit returned malformed receipt",
        };
      }
    }
    if (response.stale !== undefined) {
      return { ok: true as const, stale: true as const };
    }
    if (response.deferred !== undefined) {
      return { ok: true as const, deferred: true as const };
    }
    if (response.rejected !== undefined) {
      return { ok: true as const, rejected: true as const, errorCode: "task_notification_result_invalid" as const };
    }
    return {
      ok: false as const,
      retryable: false,
      errorCode: "bridge_commit_rejected",
      message: "task notification durable commit rejected",
    };
  }
}

function taskNotificationRejectionCode(errorCode: string): errorCode is
  | "task_notification_result_invalid"
  | "task_notification_message_invalid"
  | "task_notification_payload_mismatch" {
  return errorCode === "task_notification_result_invalid" ||
    errorCode === "task_notification_message_invalid" ||
    errorCode === "task_notification_payload_mismatch";
}

/** Configures Bridge-backed creation and closure of temporary approval-reviewer threads. */
export interface BridgeAPIApprovalReviewerThreadCreatorOptions extends BridgeDeclarationEvidenceOptions {
  readonly address: string;
  readonly tokenPath: string;
  readonly metadataFactory?: (config: ServiceAccountTokenConfig) => Promise<Metadata>;
  readonly client?: AgentRuntimeBridgeServiceClient;
}

/**
 * Manages approval-reviewer child-thread rows through Bridge. The approval reviewer calls this
 * adapter around its execution.
 */
export class BridgeAPIApprovalReviewerThreadCreator implements RuntimeApprovalReviewerThreadCreator {
  private readonly client: AgentRuntimeBridgeServiceClient;
  private readonly metadataFactory: (config: ServiceAccountTokenConfig) => Promise<Metadata>;

  constructor(private readonly options: BridgeAPIApprovalReviewerThreadCreatorOptions) {
    this.client = options.client ?? new AgentRuntimeBridgeServiceClient(options.address, credentials.createInsecure(), grpcClientChannelOptions());
    this.metadataFactory = options.metadataFactory ?? buildOutboundBearerMetadata;
  }

  /** Creates a prefixless trunk or a sidecar whose durable parent context is selected by Bridge. */
  async createApprovalReviewerThread(input: ApprovalReviewerThreadCreation) {
    let metadata: Metadata;
    try {
      metadata = await this.metadataFactory({ tokenPath: this.options.tokenPath });
    } catch {
      return { ok: false as const, message: "approval reviewer thread credential is unavailable" };
    }
    let response: EnsureApprovalReviewerTrunkResponse | EnsureApprovalReviewerSidecarResponse;
    try {
      response = input.isTrunk
        ? await ensureApprovalReviewerTrunk(this.client, {
            scope: approvalReviewerParentScope(input),
            ensureOperationId: input.ensureOperationId ?? "",
          }, metadata)
        : await ensureApprovalReviewerSidecar(this.client, {
            scope: approvalReviewerParentScope(input),
            reviewId: input.reviewId,
          }, metadata);
    } catch {
      return { ok: false as const, message: "approval reviewer thread creation is unavailable" };
    }
    if (!exactlyOneDefined(response.committed, response.duplicate)) {
      return { ok: false as const, message: "approval reviewer thread result was malformed" };
    }
    const outcome = response.committed ?? response.duplicate;
    const reviewerThreadId = outcome?.reviewerThreadId;
    if (reviewerThreadId === undefined || reviewerThreadId.length === 0) {
      return {
        ok: false as const,
        message: "approval reviewer thread was not acknowledged",
      };
    }
    let admitted: AdmitApprovalReviewInputResponse;
    try {
      admitted = await admitApprovalReviewInput(this.client, {
        scope: approvalReviewerParentScope(input),
        reviewerThreadId,
        reviewId: input.reviewId,
      }, metadata);
    } catch {
      return { ok: false as const, message: "approval reviewer input admission is unavailable" };
    }
    if (!exactlyOneDefined(admitted.committed, admitted.duplicate)) {
      return { ok: false as const, message: "approval reviewer input admission result was malformed" };
    }
    const runtimeInputId = (admitted.committed ?? admitted.duplicate)?.runtimeInputId;
    if (runtimeInputId === undefined || runtimeInputId.length === 0) {
      return { ok: false as const, message: "approval reviewer input admission result was malformed" };
    }
    return { ok: true as const, reviewerThreadId, runtimeInputId };
  }

  /** Closes the reviewer child thread and releases local scope only after Bridge acknowledges it. */
  async closeApprovalReviewerThread(input: ApprovalReviewerThreadCreation) {
    let metadata: Metadata;
    try {
      metadata = await this.metadataFactory({ tokenPath: this.options.tokenPath });
    } catch {
      return { ok: false as const, message: "approval reviewer thread credential is unavailable" };
    }
    let response: CloseApprovalReviewerResponse;
    try {
      response = await closeApprovalReviewer(this.client, {
        scope: approvalReviewerParentScope(input),
        reviewerThreadId: input.reviewerThreadId ?? "",
        reviewId: input.reviewId,
      }, metadata);
    } catch {
      return { ok: false as const, message: "approval reviewer thread close is unavailable" };
    }
    if (!exactlyOneDefined(response.committed, response.duplicate, response.stale)) {
      return { ok: false as const, message: "bridge_child_lifecycle_result_invalid" };
    }
    if (response.stale !== undefined) {
      return { ok: false as const, message: "scope_superseded", discardHotState: true };
    }
    return { ok: true as const };
  }
}

/**
 * Configures Bridge context loading and binding-token refresh.
 */
export interface BridgeAPIContextLoaderOptions extends BridgeDeclarationEvidenceOptions {
  readonly address: string;
  readonly tokenPath: string;
  readonly metadataFactory?: (config: ServiceAccountTokenConfig) => Promise<Metadata>;
  readonly client?: AgentRuntimeBridgeServiceClient;
  readonly nowEpochMs?: () => number;
  readonly refreshMarginMs?: number;
  readonly sleep?: (durationMs: number) => Promise<void>;
}

const RuntimeBindingTokenRefreshPolicy = {
  attempts: 3,
  marginMs: 60_000,
  backoffMs: [100, 300],
} as const;

/**
 * Implements Runtime Core's authenticated Bridge boundary. It loads one complete cold baseline
 * for a thread residency and separately commits accepted-input declarations; a successful commit
 * returns stamps for hot-state application and never triggers another context load. Cold reads are
 * validated before any durable state is exposed to Runtime Core.
 * It coalesces concurrent binding-token refreshes for the same binding identity.
 */
export class BridgeAPIContextLoader implements ContextLoader {
  private readonly client: AgentRuntimeBridgeServiceClient;
  private readonly metadataFactory: (config: ServiceAccountTokenConfig) => Promise<Metadata>;
  private readonly bindingTokenRefreshes = new Map<string, Promise<string>>();
  private readonly nowEpochMs: () => number;
  private readonly refreshMarginMs: number;
  private readonly sleep: (durationMs: number) => Promise<void>;
  private readonly taskNotificationCommitter: BridgeAPITaskNotificationCommitter;

  constructor(private readonly options: BridgeAPIContextLoaderOptions) {
    this.client = options.client ?? new AgentRuntimeBridgeServiceClient(options.address, credentials.createInsecure(), bridgeDurableContextGrpcChannelOptions());
    this.metadataFactory = options.metadataFactory ?? buildOutboundBearerMetadata;
    this.nowEpochMs = options.nowEpochMs ?? (() => Date.now());
    this.refreshMarginMs = options.refreshMarginMs ?? RuntimeBindingTokenRefreshPolicy.marginMs;
    this.sleep = options.sleep ?? (async (durationMs) => await new Promise<void>((resolve) => setTimeout(resolve, durationMs)));
    this.taskNotificationCommitter = new BridgeAPITaskNotificationCommitter(options);
  }

  /**
   * Returns a still-valid binding token or refreshes it through Bridge with bounded retries.
   * Concurrent refreshes for the same thread and binding share one in-flight request.
   */
  async refreshRuntimeBindingToken(
    identity: RuntimeThreadIdentity,
    options: { readonly force?: boolean | undefined } = {},
  ): Promise<string> {
    if (options.force !== true && !bindingTokenNeedsRefresh(identity.runtimeBindingToken, this.nowEpochMs(), this.refreshMarginMs)) {
      return identity.runtimeBindingToken;
    }
    const key = bindingTokenRefreshKey(identity);
    const existing = this.bindingTokenRefreshes.get(key);
    if (existing !== undefined) {
      return await existing;
    }
    const pending = this.refreshRuntimeBindingTokenOnce(identity);
    this.bindingTokenRefreshes.set(key, pending);
    try {
      return await pending;
    } finally {
      if (this.bindingTokenRefreshes.get(key) === pending) {
        this.bindingTokenRefreshes.delete(key);
      }
    }
  }

  private async refreshRuntimeBindingTokenOnce(identity: RuntimeThreadIdentity): Promise<string> {
    for (let attempt = 1; attempt <= RuntimeBindingTokenRefreshPolicy.attempts; attempt += 1) {
      try {
        const metadata = await this.metadataFactory({ tokenPath: this.options.tokenPath });
        const response = await refreshRuntimeBindingToken(this.client, {
          scope: bindingTokenRefreshScope(identity),
        }, metadata);
        if (response.runtimeBindingToken.length === 0) {
          throw new Error("runtime binding token refresh returned an empty token");
        }
        return response.runtimeBindingToken;
      } catch (error) {
        if (grpcStatusCode(error) === status.FAILED_PRECONDITION) {
          throw normalizeContextLoaderError({
            code: "superseded",
            sessionId: identity.sessionId,
            reason: "runtime binding custody is stale",
          });
        }
        if (!refreshRuntimeBindingTokenErrorRetryable(error) || attempt === RuntimeBindingTokenRefreshPolicy.attempts) {
          throw normalizeContextLoaderError({
            code: "unavailable",
            sessionId: identity.sessionId,
            reason: "runtime binding token refresh failed",
          });
        }
        await this.sleep(RuntimeBindingTokenRefreshPolicy.backoffMs[attempt - 1] ?? 0);
      }
    }
    throw normalizeContextLoaderError({
      code: "unavailable",
      sessionId: identity.sessionId,
      reason: "runtime binding token refresh failed",
    });
  }

  /** Loads and validates the complete cold-start projection for the supplied thread command. */
  async loadThreadContext(
    command: RuntimeThreadAddressState,
  ): Promise<{
    readonly messages: readonly DurableRuntimeMessage[];
    readonly turnFacts: ThreadTurnLoadFacts;
    readonly threadContextPrefix?: ThreadContextPrefix | undefined;
    readonly durableTurnId?: string | undefined;
    readonly runtimeBindingToken: string;
    readonly thread?: RuntimeAcceptedThreadMetadataState | undefined;
    readonly runtimeConfigPatch?: RuntimeConfigPatchState | undefined;
    readonly mcpManifests?: readonly RuntimeConfigPatchState[] | undefined;
    readonly pendingToolUses?: readonly RuntimeLoadedPendingToolUse[] | undefined;
    readonly pendingSandboxExecutions?: readonly RuntimePreloadedSandboxExecutionState[] | undefined;
    readonly backgroundTools?: readonly RuntimePreloadedBackgroundToolState[] | undefined;
    readonly pendingAttachments?: readonly RuntimeProviderAttachment[] | undefined;
    readonly pendingAgentMail?: readonly RuntimeLoadedAgentMail[] | undefined;
    readonly coldCoverage: RuntimeColdCoverage;
  }> {
    return await this.loadContext(command);
  }

  /** Reads target-owned durable mail without mutating sender or Inbox state. */
  async readAgentMail(command: RuntimeThreadAddressState, sourceThreadId: string): Promise<RuntimeLoadedAgentMail | undefined> {
    const response = await readAgentMail(this.client, {
      scope: bridgeScope(command),
      sourceThreadId,
    }, await this.metadata());
    if (!exactlyOneDefined(response.found, response.empty)) {
      throw normalizeContextLoaderError({
        code: "schema_mismatch",
        sessionId: command.sessionId,
        reason: "agent mail reader returned an invalid result",
      });
    }
    if (response.empty !== undefined) {
      return undefined;
    }
    const found = response.found;
    if (found === undefined || found.deliveryId.length === 0 || found.content.length === 0) {
      throw normalizeContextLoaderError({
        code: "schema_mismatch",
        sessionId: command.sessionId,
        reason: "agent mail reader returned an invalid result",
      });
    }
    return {
      deliveryId: found.deliveryId,
      message: runtimeMessageFromAgentMailContent({
        sessionId: command.sessionId,
        deliveryId: found.deliveryId,
        content: found.content,
      }),
      content: found.content,
    };
  }

  /**
   * Commits accepted input and returns only the durable receipt disposition.
   * Cold installation is the sole context read during one thread residency.
   */
  async commitAcceptedInput(
    input: RuntimeAcceptedInputState,
    options?: {
      readonly messageCreates?: readonly CoreRuntimeMessageCreate[] | undefined;
    },
  ): Promise<AcceptedInputCommitResult> {
    const messageCreates = options?.messageCreates ?? [];
    if (input.kind === "task_notification") {
      const messageCreate = messageCreates[0];
      if (messageCreates.length !== 1 || messageCreate === undefined) {
        throw normalizeContextLoaderError({
          code: "schema_mismatch",
          sessionId: input.sessionId,
          reason: "task notification declaration is incomplete",
        });
      }
      const result = await this.taskNotificationCommitter.commitTaskNotification({
        scope: input,
        command: {
          runtimeInputId: input.runtimeInputId,
          taskId: input.taskId,
          sourceToolUseEventId: input.sourceToolUseEventId,
          status: input.status,
          payloadJson: input.notificationJson,
          messageCreate,
        },
      });
      if (!result.ok) {
        throw normalizeContextLoaderError({
          code: result.retryable ? "unavailable" : "schema_mismatch",
          sessionId: input.sessionId,
          reason: result.errorCode,
        });
      }
      if ("deferred" in result) {
        return { type: "task_notification_deferred" };
      }
      if ("rejected" in result) {
        if (result.errorCode !== undefined && taskNotificationRejectionCode(result.errorCode)) {
          return { type: "task_notification_rejected", errorCode: result.errorCode };
        }
        throw normalizeContextLoaderError({
          code: "schema_mismatch",
          sessionId: input.sessionId,
          reason: "task notification rejection is invalid",
        });
      }
      if ("stale" in result) {
        return { type: "stale_custody" };
      }
      return {
        type: "receipt",
        inputDisposition: result.inputDisposition,
        applicationDisposition: "current_custody",
        receipt: result.receipt,
      };
    }
    const sourceKind = acceptedInputDeclarationKind(input);
    const metadata = await this.metadata();
    const scope = bridgeScope(input);
    const request = {
      scope,
      runtimeInputId: input.runtimeInputId,
      disposition: RuntimeInputDisposition.RUNTIME_INPUT_DISPOSITION_COMMIT,
      approvalReviewText: input.kind === "approval_review" ? orderedText(messageCreates) : [],
    };
    const declarationDigest = commitInputsDeclarationDigest(request, sourceKind);
    let response: CommitInputsResponse;
    try {
      response = await commitInputs(this.client, request, metadata);
    } catch (error) {
      const code = typeof error === "object" && error !== null && "code" in error
        ? (error as { readonly code?: unknown }).code
        : undefined;
      throw normalizeContextLoaderError({
        code: code === status.INVALID_ARGUMENT || code === status.ALREADY_EXISTS || code === status.FAILED_PRECONDITION
          ? "schema_mismatch"
          : "unavailable",
        sessionId: input.sessionId,
        reason: "commit inputs transport failed",
      });
    }
    if (!exactlyOneDefined(response.committed, response.duplicate, response.stale)) {
      throw normalizeContextLoaderError({
        code: "schema_mismatch",
        sessionId: input.sessionId,
        reason: "commit inputs returned malformed outcome",
      });
    }
    if (response.stale !== undefined) {
      return { type: "stale_custody" };
    }
    const result = response.committed ?? response.duplicate;
    if (result === undefined) {
      throw normalizeContextLoaderError({
        code: "schema_mismatch",
        sessionId: input.sessionId,
        reason: "commit inputs returned no outcome",
      });
    }
    if (!exactlyOneDefined(result.context, result.interrupt) || result.context === undefined) {
      throw normalizeContextLoaderError({
        code: "schema_mismatch",
        sessionId: input.sessionId,
        reason: "commit inputs returned mismatched application",
      });
    }
    return {
      type: "receipt",
      inputDisposition: response.duplicate === undefined ? "committed" : "duplicate",
      applicationDisposition: "current_custody",
      receipt: hotReceiptFromAssignedContext({
        sessionId: input.sessionId,
        sessionThreadId: input.sessionThreadId,
        sourceKind,
        runtimeInputId: input.runtimeInputId,
        declarationDigest,
        creates: messageCreates,
        assignedContextSequences: result.context.assignedContextSequences,
        interruptToolResults: [],
        pendingAttachmentJson: result.context.pendingAttachmentJson,
      }),
    };
  }

  private async loadContext(
    input: RuntimeThreadAddressState,
  ): Promise<{
    readonly messages: readonly DurableRuntimeMessage[];
    readonly turnFacts: ThreadTurnLoadFacts;
    readonly threadContextPrefix?: ThreadContextPrefix | undefined;
    readonly durableTurnId?: string | undefined;
    readonly runtimeBindingToken: string;
    readonly thread?: RuntimeAcceptedThreadMetadataState | undefined;
    readonly runtimeConfigPatch?: RuntimeConfigPatchState | undefined;
    readonly mcpManifests?: readonly RuntimeConfigPatchState[] | undefined;
    readonly pendingToolUses?: readonly RuntimeLoadedPendingToolUse[] | undefined;
    readonly pendingSandboxExecutions?: readonly RuntimePreloadedSandboxExecutionState[] | undefined;
    readonly backgroundTools?: readonly RuntimePreloadedBackgroundToolState[] | undefined;
    readonly pendingAttachments?: readonly RuntimeProviderAttachment[] | undefined;
    readonly pendingAgentMail?: readonly RuntimeLoadedAgentMail[] | undefined;
    readonly coldCoverage: RuntimeColdCoverage;
  }> {
    const metadata = await this.metadata();
    const response = await loadContext(this.client, {
      scope: bridgeScope(input),
      runtimeInputId: hotContextIdentity("cold_load", input.bindingId, input.bindingGeneration),
      sequenceFrom: 0,
      sequenceTo: 0,
    }, metadata);
    if (!bridgeAckAccepted(response.ack?.status)) {
      throw normalizeContextLoaderError({
        code: "unavailable",
        sessionId: input.sessionId,
        reason: response.ack?.errorCode || "load context rejected",
      });
    }
    const parsed = parseContextPayload(response.contextJson, input, this.options.logger);
    return { ...parsed, runtimeBindingToken: response.runtimeBindingToken };
  }

  private async metadata(): Promise<Metadata> {
    return await this.metadataFactory({ tokenPath: this.options.tokenPath });
  }
}

/**
 * Configures the Bridge event writer.
 */
export interface BridgeAPIEventWriterOptions extends BridgeDeclarationEvidenceOptions {
  readonly address: string;
  readonly tokenPath: string;
  readonly metadataFactory?: (config: ServiceAccountTokenConfig) => Promise<Metadata>;
  readonly client?: AgentRuntimeBridgeServiceClient;
  readonly sleep?: ((durationMs: number) => Promise<void>) | undefined;
}

/**
 * Persists Runtime Core semantic events, request ends, idle transitions, and runtime termination
 * through Bridge. Each write carries its thread scope and requires a committed or duplicate ACK;
 * rejected ACKs preserve closeout release sentinels; reviewer-outcome contract
 * rejections are deterministic and stop the shared writer retry policy.
 */
export class BridgeAPIEventWriter implements SessionEventWriter {
  private readonly client: AgentRuntimeBridgeServiceClient;
  private readonly metadataFactory: (config: ServiceAccountTokenConfig) => Promise<Metadata>;
  private readonly sleep: (durationMs: number) => Promise<void>;

  constructor(private readonly options: BridgeAPIEventWriterOptions) {
    this.client = options.client ?? new AgentRuntimeBridgeServiceClient(options.address, credentials.createInsecure(), bridgeDurableContextGrpcChannelOptions());
    this.metadataFactory = options.metadataFactory ?? buildOutboundBearerMetadata;
    this.sleep = options.sleep ?? (async (durationMs) => await new Promise<void>((resolve) => setTimeout(resolve, durationMs)));
  }

  /** Writes one semantic event and its operation-specific durable projection. */
  async append(envelope: SessionEventEnvelope): Promise<SessionEventWriterAppendResult> {
    const replayUnknownTransport = envelope.event.type === "approval_review.failure";
    try {
      const event = sessionEventForDurableWrite(envelope.event);
      const request = {
        scope: bridgeScope(envelope),
        runtimeWriteId: envelope.writeId,
        modelRequestId: envelope.modelRequestId ?? modelRequestIdForEvent(event),
        eventType: event.type,
        payloadJson: JSON.stringify(event),
        sessionVisible: false,
        contextThroughMessageSequence: envelope.contextThroughMessageSequence,
        requestKind: envelope.requestKind ?? "",
        assistantPartAppend: envelope.assistantPartAppend === undefined
          ? undefined
          : { parts: envelope.assistantPartAppend.parts.map(runtimePartCreateForBridge) },
      };
      if (WriteEventRequestMessage.encode(request).finish().byteLength > MaxBridgeDurableContextGrpcMessageBytes) {
        return eventWriterSchemaFailure(envelope.sessionId, envelope.writeId);
      }
      const metadata = await this.metadataFactory({ tokenPath: this.options.tokenPath });
      const declarationDigest = writeEventDeclarationDigest(request);
      const response = await writeEvent(this.client, request, metadata);
      if (!bridgeAckAccepted(response.ack?.status)) {
        return eventWriterRejected(
          envelope.sessionId,
          envelope.writeId,
          response.ack?.errorCode,
          event.type === "approval_review.failure",
        );
      }
      const declaration = response.declaration;
      const bridgeReceipt = declaration?.receipts[0];
      if (declaration === undefined || declaration.receipts.length !== 1 || bridgeReceipt === undefined) {
        recordBridgeReceiptEvidence(this.options, {
          workspaceId: envelope.workspaceId,
          sessionId: envelope.sessionId,
          sessionThreadId: envelope.sessionThreadId,
          operation: "write_event",
          sourceKind: event.type,
          operationId: envelope.writeId,
          declarationDigest,
          bindingId: envelope.bindingId,
          bindingGeneration: envelope.bindingGeneration,
          outcome: "receipt_shape_invalid",
        });
        return eventWriterSchemaFailure(envelope.sessionId, envelope.writeId);
      }
      const receipt = runtimeDeclarationReceipt(bridgeReceipt);
      if (
        receipt.declarationDigest !== declarationDigest ||
        receipt.events.length !== 1 ||
        receipt.events[0]?.eventId !== response.eventId ||
        receipt.events[0]?.eventSequence !== response.sequence ||
        receipt.compactedThroughMessageSequence !== undefined ||
        (
          event.type === "span.model_request_start" &&
          (
            receipt.requestStart === undefined ||
            receipt.requestStart.requestKind !== envelope.requestKind ||
            receipt.requestStart.contextThroughMessageSequence !== envelope.contextThroughMessageSequence
          )
        ) ||
        (event.type !== "span.model_request_start" && receipt.requestStart !== undefined)
      ) {
        recordBridgeReceiptEvidence(this.options, {
          workspaceId: envelope.workspaceId,
          sessionId: envelope.sessionId,
          sessionThreadId: envelope.sessionThreadId,
          operation: "write_event",
          sourceKind: event.type,
          operationId: envelope.writeId,
          declarationDigest,
          bindingId: envelope.bindingId,
          bindingGeneration: envelope.bindingGeneration,
          outcome: receipt.declarationDigest === declarationDigest
            ? "receipt_shape_invalid"
            : "declaration_digest_mismatch",
        });
        return eventWriterSchemaFailure(envelope.sessionId, envelope.writeId);
      }
      const applicationDisposition = declaration.applicationDisposition ===
        ReceiptApplicationDisposition.RECEIPT_APPLICATION_DISPOSITION_CURRENT_CUSTODY
        ? "current_custody"
        : declaration.applicationDisposition === ReceiptApplicationDisposition.RECEIPT_APPLICATION_DISPOSITION_STALE_CUSTODY
          ? "stale_custody"
          : undefined;
      if (
        applicationDisposition === undefined ||
        (
          applicationDisposition === "current_custody" &&
          (
            declaration.observedBindingId !== envelope.bindingId ||
            declaration.observedBindingGeneration !== envelope.bindingGeneration
          )
        )
      ) {
        recordBridgeReceiptEvidence(this.options, {
          workspaceId: envelope.workspaceId,
          sessionId: envelope.sessionId,
          sessionThreadId: envelope.sessionThreadId,
          operation: "write_event",
          sourceKind: event.type,
          operationId: envelope.writeId,
          declarationDigest,
          bindingId: declaration.observedBindingId,
          bindingGeneration: declaration.observedBindingGeneration,
          ...(applicationDisposition === undefined ? {} : { applicationDisposition }),
          outcome: applicationDisposition === "current_custody"
            ? "binding_identity_mismatch"
            : "receipt_shape_invalid",
        });
        return eventWriterSchemaFailure(envelope.sessionId, envelope.writeId);
      }
      recordBridgeReceiptEvidence(this.options, {
        workspaceId: envelope.workspaceId,
        sessionId: envelope.sessionId,
        sessionThreadId: envelope.sessionThreadId,
        operation: "write_event",
        sourceKind: event.type,
        operationId: envelope.writeId,
        declarationDigest,
        bindingId: declaration.observedBindingId,
        bindingGeneration: declaration.observedBindingGeneration,
        applicationDisposition,
        outcome: applicationDisposition === "current_custody" ? "applied" : "stale_custody",
      });
      return {
        ok: true,
        writeId: response.ack?.runtimeWriteId ?? "",
        eventId: response.eventId,
        processedAt: receipt.messages[0]?.updatedAt ?? new Date().toISOString(),
        declaration: {
          receipt,
          applicationDisposition,
          observedBindingId: declaration.observedBindingId,
          observedBindingGeneration: declaration.observedBindingGeneration,
        },
      };
    } catch (error) {
      return eventWriterTransportFailure(
        envelope.sessionId,
        envelope.writeId,
        error,
        replayUnknownTransport,
      );
    }
  }

  /** Settles one durable Tool target without returning payload, Event, receipt, or time echoes. */
  async settleToolResult(
    envelope: SessionEventWriterToolSettlementEnvelope,
  ): Promise<SessionEventWriterToolSettlementAttempt> {
    const request: SettleToolResultRequest = {
      scope: bridgeScope(envelope),
      settlement: runtimeToolSettlementForBridge(
        envelope.settlement.toolUseEventId,
        envelope.settlement.outcome,
      ),
    };
    let metadata: Metadata;
    try {
      metadata = await this.metadataFactory({ tokenPath: this.options.tokenPath });
    } catch (error) {
      return toolSettlementTransportFailure(envelope, error);
    }
    for (let attempt = 0; ; attempt += 1) {
      try {
        return parseToolSettlementResponse(
          await settleToolResult(this.client, request, metadata),
          envelope,
        );
      } catch (error) {
        if (!bridgeDeclarationTransportUnknown(error) || attempt >= SessionEventWriterRetryPolicy.backoffMs.length) {
          return toolSettlementTransportFailure(envelope, error);
        }
        await this.sleep(SessionEventWriterRetryPolicy.backoffMs[attempt]!);
      }
    }
  }

  /**
   * Writes a terminal model-request record, assistant seal, normalized usage, consumed
   * attachments, optional reasoning, and optional reschedule request, then validates the receipt.
  */
  async writeRequestEnd(envelope: SessionEventWriterRequestEndEnvelope): Promise<SessionEventWriterAppendResult> {
    try {
      const metadata = await this.metadataFactory({ tokenPath: this.options.tokenPath });
      const inputUncachedTokens = envelope.usage?.inputTokens ?? 0;
      const cacheReadTokens = envelope.usage?.cacheReadTokens ?? 0;
      const cacheWriteTokens = envelope.usage?.cacheWriteTokens ?? 0;
      const inputTokens = inputUncachedTokens + cacheReadTokens + cacheWriteTokens;
      const outputTokens = envelope.usage?.outputTokens ?? 0;
      const request: WriteRequestEndRequest = {
        scope: bridgeScope(envelope),
        runtimeWriteId: envelope.writeId,
        modelRequestId: envelope.modelRequestId,
        finishReason: envelope.finishReason,
        modelRequestStartEventId: envelope.modelRequestStartEventId,
        requestKind: envelope.requestKind ?? "agent_provider_request",
        isError: envelope.isError,
        errorKind: envelope.errorKind ?? "",
        consumedAttachmentRefs: [...(envelope.consumedAttachmentRefs ?? [])],
        consumedFileAttachments: (envelope.consumedFileAttachments ?? []).map((attachment) => ({
          sourceEventId: attachment.sourceEventId,
          fileId: attachment.fileId,
        })),
        reschedule: envelope.reschedule === undefined ? undefined : {
          attempt: envelope.reschedule.attempt,
          deadline: envelope.reschedule.deadline,
          backoffMs: envelope.reschedule.backoffMs,
        },
        usageJson: JSON.stringify({
          input_tokens: inputTokens,
          cache_read_input_tokens: cacheReadTokens,
          cache_creation_input_tokens: cacheWriteTokens,
          output_tokens: outputTokens,
          reasoning_output_tokens: envelope.usage?.reasoningTokens ?? 0,
          total_tokens: envelope.usage?.totalTokens ?? inputTokens + outputTokens,
          provider_usage_json: envelope.usage?.providerUsageJson ?? "{}",
        }),
        trailingPartAppend: envelope.trailingPartAppend === undefined
          ? undefined
          : { parts: envelope.trailingPartAppend.parts.map(runtimePartCreateForBridge) },
        compactionCheckpointCreate: envelope.compactionCheckpointCreate === undefined
          ? undefined
          : runtimeMessageCreateForBridge(envelope.compactionCheckpointCreate),
        prefixConsumption: envelope.prefixConsumption === undefined
          ? undefined
          : {
              childThreadId: envelope.prefixConsumption.childThreadId,
              parentBoundaryEventId: envelope.prefixConsumption.parentBoundaryEventId,
            },
        compactedThroughMessageSequence: envelope.compactedThroughMessageSequence,
        compactionEventPayloadJson: envelope.compactionEventPayloadJson ?? "",
        interruptSettlement: envelope.interruptSettlement === undefined
          ? undefined
          : { runtimeInputId: envelope.interruptSettlement.runtimeInputId },
      };
      const declarationDigest = writeRequestEndDeclarationDigest(request);
      const interruptRequest = request.interruptSettlement === undefined
        ? undefined
        : {
            scope: request.scope,
            runtimeInputId: request.interruptSettlement.runtimeInputId,
            disposition: RuntimeInputDisposition.RUNTIME_INPUT_DISPOSITION_COMMIT,
            approvalReviewText: [],
          };
      const interruptDigest = interruptRequest === undefined
        ? undefined
        : commitInputsDeclarationDigest(interruptRequest, "interrupt_control");
      const response = await writeRequestEnd(this.client, request, metadata);
      if (!bridgeAckAccepted(response.ack?.status)) {
        return eventWriterRejected(envelope.sessionId, envelope.writeId, response.ack?.errorCode);
      }
      const declaration = response.declaration;
      const expectedReceiptCount = interruptRequest === undefined ? 1 : 2;
      if (declaration === undefined || declaration.receipts.length !== expectedReceiptCount) {
        recordBridgeReceiptEvidence(this.options, {
          workspaceId: envelope.workspaceId,
          sessionId: envelope.sessionId,
          sessionThreadId: envelope.sessionThreadId,
          operation: "write_request_end",
          sourceKind: "model_request",
          operationId: envelope.modelRequestId,
          declarationDigest,
          bindingId: envelope.bindingId,
          bindingGeneration: envelope.bindingGeneration,
          outcome: "receipt_shape_invalid",
        });
        return eventWriterSchemaFailure(envelope.sessionId, envelope.writeId);
      }
      const receipts = declaration.receipts.map(runtimeDeclarationReceipt);
      const receipt = receipts.find((candidate) =>
        candidate.operationKind === "write_request_end" &&
        candidate.sourceKind === "model_request" &&
        candidate.operationId === envelope.modelRequestId
      );
      const interruptReceipt = interruptRequest === undefined
        ? undefined
        : receipts.find((candidate) =>
            candidate.operationKind === "commit_inputs" &&
            candidate.sourceKind === "interrupt_control" &&
            candidate.operationId === interruptRequest.runtimeInputId
          );
      const applicationDisposition = runtimeReceiptApplicationDisposition(declaration.applicationDisposition);
      if (
        receipt === undefined ||
        receipt.declarationDigest !== declarationDigest ||
        receipt.events.length < 1 ||
        (
          request.compactedThroughMessageSequence === undefined &&
          receipt.compactedThroughMessageSequence !== undefined
        ) ||
        (
          interruptRequest !== undefined &&
          (
            interruptReceipt === undefined ||
            interruptReceipt.declarationDigest !== interruptDigest ||
            interruptReceipt.compactedThroughMessageSequence !== undefined
          )
        ) ||
        applicationDisposition === undefined ||
        (
          applicationDisposition === "current_custody" &&
          (
            declaration.observedBindingId !== envelope.bindingId ||
            declaration.observedBindingGeneration !== envelope.bindingGeneration
          )
        )
      ) {
        recordBridgeReceiptEvidence(this.options, {
          workspaceId: envelope.workspaceId,
          sessionId: envelope.sessionId,
          sessionThreadId: envelope.sessionThreadId,
          operation: "write_request_end",
          sourceKind: "model_request",
          operationId: envelope.modelRequestId,
          declarationDigest,
          bindingId: declaration.observedBindingId,
          bindingGeneration: declaration.observedBindingGeneration,
          ...(applicationDisposition === undefined ? {} : { applicationDisposition }),
          outcome: receipt !== undefined && receipt.declarationDigest !== declarationDigest
            ? "declaration_digest_mismatch"
            : applicationDisposition === "current_custody" &&
                (
                  declaration.observedBindingId !== envelope.bindingId ||
                  declaration.observedBindingGeneration !== envelope.bindingGeneration
                )
              ? "binding_identity_mismatch"
              : "receipt_shape_invalid",
        });
        return eventWriterSchemaFailure(envelope.sessionId, envelope.writeId);
      }
      const rescheduleDisposition = bridgeRescheduleDisposition(receipt.requestReschedule);
      if (
        !rescheduleDisposition.ok ||
        (envelope.reschedule !== undefined) !== (rescheduleDisposition.value !== undefined)
      ) {
        recordBridgeReceiptEvidence(this.options, {
          workspaceId: envelope.workspaceId,
          sessionId: envelope.sessionId,
          sessionThreadId: envelope.sessionThreadId,
          operation: "write_request_end",
          sourceKind: "model_request",
          operationId: envelope.modelRequestId,
          declarationDigest,
          bindingId: declaration.observedBindingId,
          bindingGeneration: declaration.observedBindingGeneration,
          applicationDisposition,
          outcome: "receipt_shape_invalid",
        });
        return eventWriterSchemaFailure(envelope.sessionId, envelope.writeId);
      }
      recordBridgeReceiptEvidence(this.options, {
        workspaceId: envelope.workspaceId,
        sessionId: envelope.sessionId,
        sessionThreadId: envelope.sessionThreadId,
        operation: "write_request_end",
        sourceKind: "model_request",
        operationId: envelope.modelRequestId,
        declarationDigest,
        bindingId: declaration.observedBindingId,
        bindingGeneration: declaration.observedBindingGeneration,
        applicationDisposition,
        outcome: applicationDisposition === "current_custody" ? "applied" : "stale_custody",
      });
      return {
        ok: true,
        writeId: response.ack?.runtimeWriteId ?? "",
        eventId: receipt.events[0]?.eventId ?? "",
        processedAt: receipt.messages[0]?.updatedAt ?? new Date().toISOString(),
        declaration: {
          receipt,
          ...(interruptReceipt === undefined ? {} : { relatedReceipts: [interruptReceipt] }),
          applicationDisposition,
          observedBindingId: declaration.observedBindingId,
          observedBindingGeneration: declaration.observedBindingGeneration,
        },
        ...(rescheduleDisposition.value !== undefined ? { rescheduleDisposition: rescheduleDisposition.value } : {}),
      };
    } catch (error) {
      const grpcCode =
        typeof error === "object" && error !== null && "code" in error && typeof error.code === "number"
          ? error.code
          : undefined;
      return grpcCode === status.ALREADY_EXISTS
        ? eventWriterRejected(envelope.sessionId, envelope.writeId, "scope_superseded")
        : eventWriterTransportFailure(envelope.sessionId, envelope.writeId, error);
    }
  }

  /** Persists one database-named running interval's idle closeout. */
  async finishIdle(envelope: SessionEventWriterFinishIdleEnvelope): Promise<SessionEventWriterAppendResult> {
    try {
      const metadata = await this.metadataFactory({ tokenPath: this.options.tokenPath });
      const request: FinishIdleRequest = {
        scope: bridgeScope(envelope),
        durableTurnId: envelope.durableTurnId,
        stopReasonJson: JSON.stringify(envelope.stopReason),
        completionMailCreate: envelope.completionMailCreate === undefined
          ? undefined
          : runtimeMessageCreateForBridge(envelope.completionMailCreate),
      };
      const declarationDigest = finishIdleDeclarationDigest(request);
      const response = await finishIdle(this.client, request, metadata);
      if (!bridgeAckAccepted(response.ack?.status)) {
        return eventWriterRejected(envelope.sessionId, envelope.durableTurnId, response.ack?.errorCode);
      }
      const declaration = response.declaration;
      const bridgeReceipt = declaration?.receipts[0];
      if (declaration === undefined || declaration.receipts.length !== 1 || bridgeReceipt === undefined) {
        recordBridgeReceiptEvidence(this.options, {
          workspaceId: envelope.workspaceId,
          sessionId: envelope.sessionId,
          sessionThreadId: envelope.sessionThreadId,
          operation: "finish_idle",
          sourceKind: "turn_closeout",
          operationId: envelope.durableTurnId,
          declarationDigest,
          bindingId: envelope.bindingId,
          bindingGeneration: envelope.bindingGeneration,
          outcome: "receipt_shape_invalid",
        });
        return eventWriterSchemaFailure(envelope.sessionId, envelope.durableTurnId);
      }
      const receipt = runtimeDeclarationReceipt(bridgeReceipt);
      const applicationDisposition = runtimeReceiptApplicationDisposition(declaration.applicationDisposition);
      const completionCreate = envelope.completionMailCreate;
      const completionMessage = receipt.messages[0];
      const completionEvent = receipt.events[1];
      const completionReceiptMatches = completionCreate === undefined
        ? receipt.messages.length === 0 && receipt.events.length === 1
        : (
          receipt.messages.length === 1 &&
          receipt.events.length === 2 &&
          completionMessage !== undefined &&
          completionEvent !== undefined &&
          completionMessage.sessionThreadId === envelope.sessionThreadId &&
          completionMessage.parts.length === completionCreate.parts.length &&
          completionMessage.parts.every((part) => part.messageId === completionMessage.messageId)
        );
      if (
        receipt.declarationDigest !== declarationDigest ||
        !completionReceiptMatches ||
        receipt.idleCloseout === undefined ||
        receipt.idleCloseout.durableTurnId !== envelope.durableTurnId ||
        receipt.idleCloseout.idleEventId !== receipt.events[0]?.eventId ||
        receipt.idleCloseout.idleEventSequence !== receipt.events[0]?.eventSequence ||
        receipt.pendingAttachmentDelta.length !== 0 ||
        receipt.interruptToolProjections.length !== 0 ||
        receipt.prefixConsumptions.length !== 0 ||
        receipt.requestReschedule !== undefined ||
        receipt.compactedThroughMessageSequence !== undefined ||
        applicationDisposition === undefined ||
        (
          applicationDisposition === "current_custody" &&
          (
            declaration.observedBindingId !== envelope.bindingId ||
            declaration.observedBindingGeneration !== envelope.bindingGeneration
          )
        )
      ) {
        recordBridgeReceiptEvidence(this.options, {
          workspaceId: envelope.workspaceId,
          sessionId: envelope.sessionId,
          sessionThreadId: envelope.sessionThreadId,
          operation: "finish_idle",
          sourceKind: "turn_closeout",
          operationId: envelope.durableTurnId,
          declarationDigest,
          bindingId: declaration.observedBindingId,
          bindingGeneration: declaration.observedBindingGeneration,
          ...(applicationDisposition === undefined ? {} : { applicationDisposition }),
          outcome: receipt.declarationDigest !== declarationDigest
            ? "declaration_digest_mismatch"
            : applicationDisposition === "current_custody" &&
                (
                  declaration.observedBindingId !== envelope.bindingId ||
                  declaration.observedBindingGeneration !== envelope.bindingGeneration
                )
              ? "binding_identity_mismatch"
              : "receipt_shape_invalid",
        });
        return eventWriterSchemaFailure(envelope.sessionId, envelope.durableTurnId);
      }
      recordBridgeReceiptEvidence(this.options, {
        workspaceId: envelope.workspaceId,
        sessionId: envelope.sessionId,
        sessionThreadId: envelope.sessionThreadId,
        operation: "finish_idle",
        sourceKind: "turn_closeout",
        operationId: envelope.durableTurnId,
        declarationDigest,
        bindingId: declaration.observedBindingId,
        bindingGeneration: declaration.observedBindingGeneration,
        applicationDisposition,
        outcome: applicationDisposition === "current_custody" ? "applied" : "stale_custody",
      });
      return {
        ok: true,
        writeId: response.ack?.runtimeWriteId ?? "",
        eventId: receipt.idleCloseout.idleEventId,
        processedAt: receipt.idleCloseout.committedIdleAt,
        declaration: {
          receipt,
          applicationDisposition,
          observedBindingId: declaration.observedBindingId,
          observedBindingGeneration: declaration.observedBindingGeneration,
        },
      };
    } catch (error) {
      return eventWriterTransportFailure(envelope.sessionId, envelope.durableTurnId, error);
    }
  }

  /** Commits atomic runtime termination closeout for the active thread scope. */
  async commitRuntimeTermination(envelope: SessionEventWriterRuntimeTerminationEnvelope): Promise<SessionEventWriterRuntimeTerminationResult> {
    try {
      const metadata = await this.metadataFactory({ tokenPath: this.options.tokenPath });
      const request: CommitRuntimeTerminationRequest = {
        scope: bridgeScope(envelope),
        runtimeWriteId: envelope.writeId,
        failureJson: JSON.stringify(envelope.failure),
      };
      const response = await commitRuntimeTermination(this.client, request, metadata);
      return parseRuntimeTerminationResult(response);
    } catch (error) {
      if (error instanceof RuntimeTerminationResultContractError) {
        return { ok: false, error: normalizeSessionEventWriterError({ code: "schema_mismatch", sessionId: envelope.sessionId, writeId: envelope.writeId }) };
      }
      const failure = eventWriterTransportFailure(envelope.sessionId, envelope.writeId, error);
      return failure.ok ? { ok: false, error: normalizeSessionEventWriterError({ code: "unknown", sessionId: envelope.sessionId, writeId: envelope.writeId }) } : failure;
    }
  }

}

function bridgeRescheduleDisposition(
  stamp: RuntimeDeclarationReceipt["requestReschedule"],
):
  | { readonly ok: true; readonly value?: {
      readonly status: "accepted";
      readonly attempt: number;
      readonly effectiveDeadline: string;
    } | {
      readonly status: "denied";
      readonly reason: "attempt_mismatch" | "budget_exhausted";
      readonly attempt: number;
    } }
  | { readonly ok: false; readonly reason: string } {
  if (stamp === undefined) {
    return { ok: true };
  }
  if (stamp.disposition === "accepted") {
    if (!Number.isSafeInteger(stamp.attempt) || stamp.attempt <= 0 ||
      stamp.effectiveDeadline.length === 0 || !Number.isFinite(Date.parse(stamp.effectiveDeadline))) {
      return { ok: false, reason: "accepted reschedule disposition is malformed" };
    }
    return {
      ok: true,
      value: {
        status: "accepted",
        attempt: stamp.attempt,
        effectiveDeadline: stamp.effectiveDeadline,
      },
    };
  }
  if (stamp.disposition === "denied_attempt_mismatch" || stamp.disposition === "denied_budget_exhausted") {
    if (!Number.isSafeInteger(stamp.attempt) || stamp.attempt < 0) {
      return { ok: false, reason: "denied reschedule disposition is malformed" };
    }
    return {
      ok: true,
      value: {
        status: "denied",
        reason: stamp.disposition === "denied_attempt_mismatch" ? "attempt_mismatch" : "budget_exhausted",
        attempt: stamp.attempt,
      },
    };
  }
  return { ok: false, reason: "reschedule disposition status is malformed" };
}

function runtimeReceiptApplicationDisposition(
  disposition: ReceiptApplicationDisposition,
): "current_custody" | "stale_custody" | undefined {
  if (disposition === ReceiptApplicationDisposition.RECEIPT_APPLICATION_DISPOSITION_CURRENT_CUSTODY) {
    return "current_custody";
  }
  if (disposition === ReceiptApplicationDisposition.RECEIPT_APPLICATION_DISPOSITION_STALE_CUSTODY) {
    return "stale_custody";
  }
  return undefined;
}

/** Configures durable internal-tool repair writes against active thread binding scopes. */
export interface BridgeAPIInternalToolRepairCommitterOptions extends BridgeDeclarationEvidenceOptions {
  readonly address: string;
  readonly tokenPath: string;
  readonly metadataFactory?: (config: ServiceAccountTokenConfig) => Promise<Metadata>;
  readonly client?: AgentRuntimeBridgeServiceClient;
}

/**
 * Persists Runtime Core's self-contained invalid-tool repair through Bridge. It maps missing scope,
 * authentication, transport, and conflicting ACKs into the message-store result vocabulary.
 */
export class BridgeAPIInternalToolRepairCommitter {
  private readonly client: AgentRuntimeBridgeServiceClient;
  private readonly metadataFactory: (config: ServiceAccountTokenConfig) => Promise<Metadata>;

  constructor(private readonly options: BridgeAPIInternalToolRepairCommitterOptions) {
    this.client = options.client ?? new AgentRuntimeBridgeServiceClient(options.address, credentials.createInsecure(), grpcClientChannelOptions());
    this.metadataFactory = options.metadataFactory ?? buildOutboundBearerMetadata;
  }

  /** Commits one unstamped repair message and returns its database receipt. */
  async commitInternalToolRepair(repair: RuntimeInternalToolRepairCommit): Promise<RuntimeInternalToolRepairCommitResult> {
    let metadata: Metadata;
    try {
      metadata = await this.metadataFactory({ tokenPath: this.options.tokenPath });
    } catch {
      return internalToolRepairStoreFailure("unavailable", repair.sessionId);
    }
    const request = {
      scope: bridgeScope(repair),
      modelRequestId: repair.modelRequestId,
      modelToolCallId: repair.modelToolCallId,
      toolName: repair.toolName,
      messageCreate: runtimeMessageCreateForBridge(repair.messageCreate),
    };
    const declarationDigest = internalToolRepairDeclarationDigest(request, repair.repairKey);
    let response: CommitInternalToolRepairResponse;
    try {
      response = await commitInternalToolRepair(this.client, request, metadata);
    } catch (error) {
      return internalToolRepairStoreFailure(bridgeStoreErrorCode(error), repair.sessionId);
    }
    if (!bridgeAckAccepted(response.ack?.status)) {
      const code = response.ack?.errorCode === "internal_tool_repair_conflict" ? "constraint_violation" : "unavailable";
      return internalToolRepairStoreFailure(code, repair.sessionId, response.ack?.errorCode);
    }
    const declaration = response.declaration;
    const bridgeReceipt = declaration?.receipts[0];
    if (
      declaration === undefined ||
      declaration.receipts.length !== 1 ||
      bridgeReceipt === undefined
    ) {
      recordBridgeReceiptEvidence(this.options, {
        workspaceId: repair.workspaceId,
        sessionId: repair.sessionId,
        sessionThreadId: repair.sessionThreadId,
        operation: "commit_internal_tool_repair",
        sourceKind: "internal_tool_repair",
        operationId: repair.repairKey,
        declarationDigest,
        bindingId: repair.bindingId,
        bindingGeneration: repair.bindingGeneration,
        outcome: "receipt_shape_invalid",
      });
      return internalToolRepairStoreFailure("unavailable", repair.sessionId);
    }
    const receipt = runtimeDeclarationReceipt(bridgeReceipt);
    const applicationDisposition = declaration.applicationDisposition ===
      ReceiptApplicationDisposition.RECEIPT_APPLICATION_DISPOSITION_CURRENT_CUSTODY
      ? "current_custody"
      : declaration.applicationDisposition === ReceiptApplicationDisposition.RECEIPT_APPLICATION_DISPOSITION_STALE_CUSTODY
        ? "stale_custody"
        : undefined;
    if (
      receipt.declarationDigest !== declarationDigest ||
      receipt.events.length !== 1 ||
      receipt.events[0]?.eventId === undefined ||
      receipt.compactedThroughMessageSequence !== undefined ||
      applicationDisposition === undefined ||
      (
        applicationDisposition === "current_custody" &&
        (
          declaration.observedBindingId !== repair.bindingId ||
          declaration.observedBindingGeneration !== repair.bindingGeneration
        )
      )
    ) {
      recordBridgeReceiptEvidence(this.options, {
        workspaceId: repair.workspaceId,
        sessionId: repair.sessionId,
        sessionThreadId: repair.sessionThreadId,
        operation: "commit_internal_tool_repair",
        sourceKind: "internal_tool_repair",
        operationId: repair.repairKey,
        declarationDigest,
        bindingId: declaration.observedBindingId,
        bindingGeneration: declaration.observedBindingGeneration,
        ...(applicationDisposition === undefined ? {} : { applicationDisposition }),
        outcome: receipt.declarationDigest !== declarationDigest
          ? "declaration_digest_mismatch"
          : applicationDisposition === "current_custody" &&
              (
                declaration.observedBindingId !== repair.bindingId ||
                declaration.observedBindingGeneration !== repair.bindingGeneration
              )
            ? "binding_identity_mismatch"
            : "receipt_shape_invalid",
      });
      return internalToolRepairStoreFailure("unavailable", repair.sessionId);
    }
    recordBridgeReceiptEvidence(this.options, {
      workspaceId: repair.workspaceId,
      sessionId: repair.sessionId,
      sessionThreadId: repair.sessionThreadId,
      operation: "commit_internal_tool_repair",
      sourceKind: "internal_tool_repair",
      operationId: repair.repairKey,
      declarationDigest,
      bindingId: declaration.observedBindingId,
      bindingGeneration: declaration.observedBindingGeneration,
      applicationDisposition,
      outcome: applicationDisposition === "current_custody" ? "applied" : "stale_custody",
    });
    return {
      ok: true,
      eventId: receipt.events[0].eventId,
      declaration: {
        receipt,
        applicationDisposition,
        observedBindingId: declaration.observedBindingId,
        observedBindingGeneration: declaration.observedBindingGeneration,
      },
    };
  }
}

function commitTaskNotificationResult(
  client: AgentRuntimeBridgeServiceClient,
  request: Parameters<AgentRuntimeBridgeServiceClient["commitTaskNotificationResult"]>[0],
  metadata: Metadata,
): Promise<CommitTaskNotificationResultResponse> {
  return new Promise((resolve, reject) => {
    client.commitTaskNotificationResult(request, metadata, (error: ServiceError | null, response: CommitTaskNotificationResultResponse) => {
      if (error !== null) {
        reject(error);
        return;
      }
      resolve(response);
    });
  });
}

function commitInternalToolRepair(
  client: AgentRuntimeBridgeServiceClient,
  request: CommitInternalToolRepairRequest,
  metadata: Metadata,
): Promise<CommitInternalToolRepairResponse> {
  return new Promise((resolve, reject) => {
    client.commitInternalToolRepair(request, metadata, (error: ServiceError | null, response: CommitInternalToolRepairResponse) => {
      if (error !== null) {
        reject(error);
        return;
      }
      resolve(response);
    });
  });
}

function commitInputs(
  client: AgentRuntimeBridgeServiceClient,
  request: CommitInputsRequest,
  metadata: Metadata,
): Promise<CommitInputsResponse> {
  return new Promise((resolve, reject) => {
    const options: CallOptions = {
      deadline: Date.now() + SessionEventWriterRetryPolicy.timeoutPerAttemptMs,
    };
    client.commitInputs(request, metadata, options, (error: ServiceError | null, response: CommitInputsResponse) => {
      if (error !== null) {
        reject(error);
        return;
      }
      resolve(response);
    });
  });
}

function ensureApprovalReviewerTrunk(
  client: AgentRuntimeBridgeServiceClient,
  request: EnsureApprovalReviewerTrunkRequest,
  metadata: Metadata,
): Promise<EnsureApprovalReviewerTrunkResponse> {
  return new Promise((resolve, reject) => {
    client.ensureApprovalReviewerTrunk(request, metadata, (error: ServiceError | null, response: EnsureApprovalReviewerTrunkResponse) => {
      if (error !== null) {
        reject(error);
        return;
      }
      resolve(response);
    });
  });
}

function ensureApprovalReviewerSidecar(
  client: AgentRuntimeBridgeServiceClient,
  request: EnsureApprovalReviewerSidecarRequest,
  metadata: Metadata,
): Promise<EnsureApprovalReviewerSidecarResponse> {
  return new Promise((resolve, reject) => {
    client.ensureApprovalReviewerSidecar(request, metadata, (error: ServiceError | null, response: EnsureApprovalReviewerSidecarResponse) => {
      if (error !== null) {
        reject(error);
        return;
      }
      resolve(response);
    });
  });
}

function admitApprovalReviewInput(
  client: AgentRuntimeBridgeServiceClient,
  request: AdmitApprovalReviewInputRequest,
  metadata: Metadata,
): Promise<AdmitApprovalReviewInputResponse> {
  return new Promise((resolve, reject) => {
    client.admitApprovalReviewInput(request, metadata, (error: ServiceError | null, response: AdmitApprovalReviewInputResponse) => {
      if (error !== null) {
        reject(error);
        return;
      }
      resolve(response);
    });
  });
}

function closeApprovalReviewer(
  client: AgentRuntimeBridgeServiceClient,
  request: CloseApprovalReviewerRequest,
  metadata: Metadata,
): Promise<CloseApprovalReviewerResponse> {
  return new Promise((resolve, reject) => {
    client.closeApprovalReviewer(request, metadata, (error: ServiceError | null, response: CloseApprovalReviewerResponse) => {
      if (error !== null) {
        reject(error);
        return;
      }
      resolve(response);
    });
  });
}

function readAgentMail(
  client: AgentRuntimeBridgeServiceClient,
  request: ReadAgentMailRequest,
  metadata: Metadata,
): Promise<ReadAgentMailResponse> {
  return new Promise((resolve, reject) => {
    client.readAgentMail(request, metadata, (error: ServiceError | null, response: ReadAgentMailResponse) => {
      if (error !== null) {
        reject(error);
        return;
      }
      resolve(response);
    });
  });
}

function loadContext(
  client: AgentRuntimeBridgeServiceClient,
  request: LoadContextRequest,
  metadata: Metadata,
): Promise<LoadContextResponse> {
  return new Promise((resolve, reject) => {
    client.loadContext(request, metadata, (error: ServiceError | null, response: LoadContextResponse) => {
      if (error !== null) {
        reject(error);
        return;
      }
      resolve(response);
    });
  });
}

function runtimeMessageCreateForBridge(create: CoreRuntimeMessageCreate) {
  const {
    messageKind,
    parts,
    ...messageInfo
  } = create;
  return {
    messageKind: runtimeMessageCreateKindForBridge(messageKind),
    messageInfoJson: JSON.stringify(messageInfo),
    parts: parts.map(runtimePartCreateForBridge),
  };
}

function runtimePartCreateForBridge(part: CoreRuntimePartCreate) {
  return { partKind: part.type, partJson: JSON.stringify(part) };
}

function runtimeMessageCreateKindForBridge(kind: CoreRuntimeMessageCreate["messageKind"]): RuntimeMessageCreateKind {
  switch (kind) {
    case "user_input":
      return RuntimeMessageCreateKind.RUNTIME_MESSAGE_CREATE_KIND_USER_INPUT;
    case "approval_input":
      return RuntimeMessageCreateKind.RUNTIME_MESSAGE_CREATE_KIND_APPROVAL_INPUT;
    case "reviewer_input":
      return RuntimeMessageCreateKind.RUNTIME_MESSAGE_CREATE_KIND_REVIEWER_INPUT;
    case "agent_mail_input":
      return RuntimeMessageCreateKind.RUNTIME_MESSAGE_CREATE_KIND_AGENT_MAIL_INPUT;
    case "task_notification":
      return RuntimeMessageCreateKind.RUNTIME_MESSAGE_CREATE_KIND_TASK_NOTIFICATION;
    case "rejection":
      return RuntimeMessageCreateKind.RUNTIME_MESSAGE_CREATE_KIND_REJECTION;
    case "completion_mail":
      return RuntimeMessageCreateKind.RUNTIME_MESSAGE_CREATE_KIND_COMPLETION_MAIL;
    case "compaction_checkpoint":
      return RuntimeMessageCreateKind.RUNTIME_MESSAGE_CREATE_KIND_COMPACTION_CHECKPOINT;
    case "internal_tool_repair":
      return RuntimeMessageCreateKind.RUNTIME_MESSAGE_CREATE_KIND_INTERNAL_TOOL_REPAIR;
    case "termination":
      return RuntimeMessageCreateKind.RUNTIME_MESSAGE_CREATE_KIND_TERMINATION;
  }
}

function runtimeToolSettlementForBridge(toolUseEventId: string, settlement: RuntimeToolSettlementDeclaration["outcome"]) {
  switch (settlement.type) {
    case "completed":
      return {
        toolUseEventId,
        completed: {
          outputJson: JSON.stringify(settlement.output),
          serverToolUse: settlement.serverToolUse,
        },
      };
    case "error":
      return {
        toolUseEventId,
        error: {
          errorJson: JSON.stringify(runtimeToolErrorFromFailure(settlement.error)),
          serverToolUse: settlement.serverToolUse,
        },
      };
    case "cancelled":
      return {
        toolUseEventId,
        cancelled: settlement.error === undefined ? {} : { errorJson: JSON.stringify(runtimeToolErrorFromFailure(settlement.error)) },
      };
  }
}

function ordinaryRuntimeDeclarationReceipt(receipt: BridgeDeclarationReceipt): RuntimeDeclarationReceipt {
  const parsed = runtimeDeclarationReceipt(receipt);
  if (parsed.compactedThroughMessageSequence !== undefined) {
    throw new Error("ordinary declaration receipt cannot carry a compaction boundary");
  }
  return parsed;
}

function runtimeDeclarationReceipt(receipt: BridgeDeclarationReceipt): RuntimeDeclarationReceipt {
  const requestReschedule = receipt.requestReschedule === undefined
    ? undefined
    : runtimeRequestRescheduleStamp(receipt.requestReschedule);
  const idleCloseout = receipt.idleCloseout === undefined
    ? undefined
    : {
        durableTurnId: receipt.idleCloseout.durableTurnId,
        idleEventId: receipt.idleCloseout.idleEventId,
        idleEventSequence: receipt.idleCloseout.idleEventSequence,
        committedIdleAt: receipt.idleCloseout.committedIdleAt,
      };
  const requestStart = receipt.requestStart === undefined
    ? undefined
    : {
        requestKind: parseRuntimeRequestKind(receipt.requestStart.requestKind),
        contextThroughMessageSequence: receipt.requestStart.contextThroughMessageSequence,
      };
  return {
    sessionThreadId: receipt.sessionThreadId,
    operationKind: receipt.operationKind,
    sourceKind: receipt.sourceKind,
    operationId: receipt.operationId,
    declarationDigest: receipt.declarationDigest,
    pendingAttachmentDelta: parsePendingAttachments(
      receipt.pendingAttachmentDeltaJson.map((item) => JSON.parse(item) as unknown),
    ) ?? [],
    interruptToolProjections: receipt.interruptToolProjections.map((projection) => {
      if (projection.resultEvent === undefined) {
        throw new Error("interrupt Tool projection has no result event");
      }
      const terminalState = projection.error !== undefined
        ? { type: "error" as const, error: RuntimeToolErrorSchema.parse(JSON.parse(projection.error.errorJson)) }
        : projection.cancelled !== undefined
          ? {
              type: "cancelled" as const,
              ...(projection.cancelled.errorJson === undefined
                ? {}
                : { error: RuntimeToolErrorSchema.parse(JSON.parse(projection.cancelled.errorJson)) }),
            }
          : undefined;
      if (terminalState === undefined) {
        throw new Error("interrupt Tool projection has no terminal state");
      }
      return {
        toolUseEventId: projection.toolUseEventId,
        resultEvent: {
          sessionThreadId: projection.resultEvent.sessionThreadId,
          eventId: projection.resultEvent.eventId,
          eventSequence: projection.resultEvent.eventSequence,
          disposition: runtimeEventDisposition(projection.resultEvent.disposition),
        },
        terminalState,
      };
    }),
    prefixConsumptions: receipt.prefixConsumptions.map((consumption) => {
      if (
        consumption.disposition !==
        PrefixConsumptionDisposition.PREFIX_CONSUMPTION_DISPOSITION_CONSUMED
      ) {
        throw new Error("declaration receipt has an invalid prefix-consumption disposition");
      }
      return {
        childThreadId: consumption.childThreadId,
        parentBoundaryEventId: consumption.parentBoundaryEventId,
        checkpointMessageId: consumption.checkpointMessageId,
        disposition: "consumed" as const,
      };
    }),
    ...(requestReschedule !== undefined ? { requestReschedule } : {}),
    ...(requestStart !== undefined ? { requestStart } : {}),
    ...(idleCloseout !== undefined ? { idleCloseout } : {}),
    ...(receipt.compactedThroughMessageSequence !== undefined
      ? { compactedThroughMessageSequence: receipt.compactedThroughMessageSequence }
      : {}),
    childLifecycle: receipt.childLifecycle.map((stamp) => {
      const disposition = stamp.disposition === ChildLifecycleDisposition.CHILD_LIFECYCLE_DISPOSITION_CLOSED
        ? "closed" as const
        : stamp.disposition === ChildLifecycleDisposition.CHILD_LIFECYCLE_DISPOSITION_ALREADY_CLOSED
          ? "already_closed" as const
          : stamp.disposition === ChildLifecycleDisposition.CHILD_LIFECYCLE_DISPOSITION_RESUMED
            ? "resumed" as const
            : stamp.disposition === ChildLifecycleDisposition.CHILD_LIFECYCLE_DISPOSITION_ALREADY_ACTIVE
              ? "already_active" as const
              : stamp.disposition === ChildLifecycleDisposition.CHILD_LIFECYCLE_DISPOSITION_PRESERVED_FAILED
                ? "preserved_failed" as const
                : stamp.disposition === ChildLifecycleDisposition.CHILD_LIFECYCLE_DISPOSITION_PRESERVED_TERMINATED
                  ? "preserved_terminated" as const
                  : undefined;
      if (disposition === undefined || stamp.childThreadId.length === 0 || stamp.effectiveAt.length === 0) {
        throw new Error("declaration receipt has an invalid child-lifecycle stamp");
      }
      return {
        childThreadId: stamp.childThreadId,
        disposition,
        effectiveAt: stamp.effectiveAt,
      };
    }),
    events: receipt.events.map((event) => ({
      sessionThreadId: event.sessionThreadId,
      eventId: event.eventId,
      eventSequence: event.eventSequence,
      disposition: runtimeEventDisposition(event.disposition),
    })),
    messages: receipt.messages.map((message) => ({
      sessionThreadId: message.sessionThreadId,
      messageId: message.messageId,
      messageSequence: message.messageSequence,
      createdAt: message.createdAt,
      updatedAt: message.updatedAt,
      disposition: runtimeProjectionDisposition(message.disposition),
      parts: message.parts.map((part) => ({
        partId: part.partId,
        messageId: part.messageId,
        partSequence: part.partSequence,
        createdAt: part.createdAt,
        updatedAt: part.updatedAt,
        disposition: runtimeProjectionDisposition(part.disposition),
      })),
    })),
  };
}

function parseRuntimeRequestKind(
  value: string,
): "agent_provider_request" | "compaction_summary" | "approval_reviewer" {
  if (value === "agent_provider_request" || value === "compaction_summary" || value === "approval_reviewer") {
    return value;
  }
  throw new Error("declaration receipt has an invalid request-start stamp");
}

function runtimeRequestRescheduleStamp(
  stamp: NonNullable<BridgeDeclarationReceipt["requestReschedule"]>,
): NonNullable<RuntimeDeclarationReceipt["requestReschedule"]> {
  const disposition = stamp.disposition === RequestRescheduleDisposition.REQUEST_RESCHEDULE_DISPOSITION_ACCEPTED
    ? "accepted" as const
    : stamp.disposition === RequestRescheduleDisposition.REQUEST_RESCHEDULE_DISPOSITION_DENIED_ATTEMPT_MISMATCH
      ? "denied_attempt_mismatch" as const
      : stamp.disposition === RequestRescheduleDisposition.REQUEST_RESCHEDULE_DISPOSITION_DENIED_BUDGET_EXHAUSTED
        ? "denied_budget_exhausted" as const
        : undefined;
  if (
    disposition === undefined ||
    !["agent_provider_request", "compaction_summary", "approval_reviewer"].includes(stamp.requestKind)
  ) {
    throw new Error("declaration receipt has an invalid request-reschedule stamp");
  }
  return {
    disposition,
    requestKind: stamp.requestKind as "agent_provider_request" | "compaction_summary" | "approval_reviewer",
    attempt: stamp.attempt,
    effectiveDeadline: stamp.effectiveDeadline,
  };
}

function runtimeEventDisposition(
  disposition: DurableEventDisposition,
): RuntimeDeclarationReceipt["events"][number]["disposition"] {
  if (disposition === DurableEventDisposition.DURABLE_EVENT_DISPOSITION_EXISTING) {
    return "existing";
  }
  if (disposition === DurableEventDisposition.DURABLE_EVENT_DISPOSITION_CREATED) {
    return "created";
  }
  throw new Error("declaration receipt has an invalid event disposition");
}

function runtimeProjectionDisposition(
  disposition: DurableProjectionDisposition,
): RuntimeDeclarationReceipt["messages"][number]["disposition"] {
  if (disposition === DurableProjectionDisposition.DURABLE_PROJECTION_DISPOSITION_CREATED) {
    return "created";
  }
  if (disposition === DurableProjectionDisposition.DURABLE_PROJECTION_DISPOSITION_UPDATED) {
    return "updated";
  }
  throw new Error("declaration receipt has an invalid projection disposition");
}

function writeEvent(
  client: AgentRuntimeBridgeServiceClient,
  request: WriteEventRequest,
  metadata: Metadata,
): Promise<WriteEventResponse> {
  return new Promise((resolve, reject) => {
    client.writeEvent(request, metadata, (error: ServiceError | null, response: WriteEventResponse) => {
      if (error !== null) {
        reject(error);
        return;
      }
      resolve(response);
    });
  });
}

function settleToolResult(
  client: AgentRuntimeBridgeServiceClient,
  request: SettleToolResultRequest,
  metadata: Metadata,
): Promise<SettleToolResultResponse> {
  return new Promise((resolve, reject) => {
    const options: CallOptions = {
      deadline: Date.now() + SessionEventWriterRetryPolicy.timeoutPerAttemptMs,
    };
    client.settleToolResult(request, metadata, options, (error: ServiceError | null, response: SettleToolResultResponse) => {
      if (error !== null) {
        reject(error);
        return;
      }
      resolve(response);
    });
  });
}

function writeRequestEnd(
  client: AgentRuntimeBridgeServiceClient,
  request: WriteRequestEndRequest,
  metadata: Metadata,
): Promise<WriteRequestEndResponse> {
  return new Promise((resolve, reject) => {
    client.writeRequestEnd(request, metadata, (error: ServiceError | null, response: WriteRequestEndResponse) => {
      if (error !== null) {
        reject(error);
        return;
      }
      resolve(response);
    });
  });
}

function finishIdle(
  client: AgentRuntimeBridgeServiceClient,
  request: FinishIdleRequest,
  metadata: Metadata,
): Promise<FinishIdleResponse> {
  return new Promise((resolve, reject) => {
    client.finishIdle(request, metadata, (error: ServiceError | null, response: FinishIdleResponse) => {
      if (error !== null) {
        reject(error);
        return;
      }
      resolve(response);
    });
  });
}

function commitRuntimeTermination(
  client: AgentRuntimeBridgeServiceClient,
  request: CommitRuntimeTerminationRequest,
  metadata: Metadata,
): Promise<CommitRuntimeTerminationResponse> {
  return new Promise((resolve, reject) => {
    client.commitRuntimeTermination(request, metadata, (error: ServiceError | null, response: CommitRuntimeTerminationResponse) => {
      if (error !== null) {
        reject(error);
        return;
      }
      resolve(response);
    });
  });
}

function refreshRuntimeBindingToken(
  client: AgentRuntimeBridgeServiceClient,
  request: RefreshRuntimeBindingTokenRequest,
  metadata: Metadata,
): Promise<RefreshRuntimeBindingTokenResponse> {
  return new Promise((resolve, reject) => {
    client.refreshRuntimeBindingToken(request, metadata, (error: ServiceError | null, response: RefreshRuntimeBindingTokenResponse) => {
      if (error !== null) {
        reject(error);
        return;
      }
      resolve(response);
    });
  });
}

function bridgeScope(input: {
  readonly workspaceId: string;
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly bindingId: string;
  readonly bindingGeneration: number;
  readonly targetPodUid: string;
}): RuntimeScope {
  return {
    workspaceId: input.workspaceId,
    sessionId: input.sessionId,
    sessionThreadId: input.sessionThreadId,
    binding: {
      bindingId: input.bindingId,
      bindingGeneration: input.bindingGeneration,
      targetPodUid: input.targetPodUid,
    },
  };
}

function bindingTokenRefreshScope(identity: RuntimeThreadIdentity): RuntimeScope {
  return {
    workspaceId: identity.workspaceId,
    sessionId: identity.sessionId,
    sessionThreadId: identity.sessionThreadId,
    binding: {
      bindingId: identity.bindingId,
      bindingGeneration: identity.bindingGeneration,
      targetPodUid: identity.targetPodUid,
    },
  };
}

function bindingTokenRefreshKey(identity: RuntimeThreadIdentity): string {
  return [
    identity.workspaceId,
    identity.sessionId,
    identity.sessionThreadId,
    identity.bindingId,
    identity.bindingGeneration,
    identity.targetPodUid,
  ].join("\u0000");
}

function bindingTokenNeedsRefresh(token: string, nowEpochMs: number, marginMs: number): boolean {
  const payload = token.split(".")[1];
  if (payload === undefined) {
    return true;
  }
  try {
    const parsed = JSON.parse(Buffer.from(payload, "base64url").toString("utf8")) as unknown;
    if (!isRecord(parsed) || typeof parsed.exp !== "number" || !Number.isFinite(parsed.exp)) {
      return true;
    }
    return parsed.exp * 1_000 - nowEpochMs <= Math.max(0, marginMs);
  } catch {
    return true;
  }
}

function refreshRuntimeBindingTokenErrorRetryable(error: unknown): boolean {
  const code = grpcStatusCode(error);
  return code === status.UNAVAILABLE || code === status.DEADLINE_EXCEEDED;
}

function grpcStatusCode(error: unknown): unknown {
  return typeof error === "object" && error !== null && "code" in error
    ? (error as { readonly code?: unknown }).code
    : undefined;
}

function approvalReviewerParentScope(input: ApprovalReviewerThreadCreation): RuntimeScope {
  return {
    workspaceId: input.request.workspaceId,
    sessionId: input.request.sessionId,
    sessionThreadId: input.request.sessionThreadId,
    binding: {
      bindingId: input.request.bindingId,
      bindingGeneration: input.request.bindingGeneration,
      targetPodUid: input.request.targetPodUid,
    },
  };
}

function bridgeAckAccepted(status: BridgeWriteStatus | undefined): boolean {
  return status === BridgeWriteStatus.BRIDGE_WRITE_STATUS_COMMITTED || status === BridgeWriteStatus.BRIDGE_WRITE_STATUS_DUPLICATE;
}

function hotReceiptFromAssignedContext(input: {
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly sourceKind: string;
  readonly runtimeInputId: string;
  readonly declarationDigest: string;
  readonly creates: readonly CoreRuntimeMessageCreate[];
  readonly assignedContextSequences: readonly number[];
  readonly interruptToolResults: readonly RuntimeInterruptToolResult[];
  readonly pendingAttachmentJson: readonly string[];
}): RuntimeDeclarationReceipt {
  if (
    input.creates.length !== input.assignedContextSequences.length ||
    input.assignedContextSequences.some((sequence) => !Number.isSafeInteger(sequence) || sequence <= 0)
  ) {
    throw new Error("Bridge assigned context sequences do not match the accepted input projection");
  }
  const timestamp = "1970-01-01T00:00:00.000Z";
  const events = input.creates.map((_, index) => ({
    sessionThreadId: input.sessionThreadId,
    eventId: hotContextIdentity("event", input.runtimeInputId, index),
    eventSequence: input.assignedContextSequences[index] ?? index + 1,
    disposition: "created" as const,
  }));
  const messages = input.creates.map((create, index) => {
    const messageId = hotContextIdentity("message", input.runtimeInputId, index);
    return {
      sessionThreadId: input.sessionThreadId,
      messageId,
      messageSequence: input.assignedContextSequences[index]!,
      createdAt: timestamp,
      updatedAt: timestamp,
      disposition: "created" as const,
      parts: create.parts.map((_, partIndex) => ({
        partId: hotContextIdentity("part", input.runtimeInputId, index, partIndex),
        messageId,
        partSequence: partIndex,
        createdAt: timestamp,
        updatedAt: timestamp,
        disposition: "created" as const,
      })),
    };
  });
  const interruptToolProjections = input.interruptToolResults.map((result, index) => {
    if (!exactlyOneDefined(result.error, result.cancelled)) {
      throw new Error("Bridge interrupt result must contain exactly one outcome");
    }
    const error = result.error === undefined
      ? undefined
      : RuntimeToolErrorSchema.parse(JSON.parse(result.error.errorJson));
    const cancelledError = result.cancelled?.errorJson === undefined
      ? undefined
      : RuntimeToolErrorSchema.parse(JSON.parse(result.cancelled.errorJson));
    if (error === undefined && result.cancelled === undefined) {
      throw new Error("Bridge interrupt result is malformed");
    }
    return {
      toolUseEventId: result.toolUseEventId,
      resultEvent: {
        sessionThreadId: input.sessionThreadId,
        eventId: hotContextIdentity("interrupt", input.runtimeInputId, index),
        eventSequence: index + 1,
        disposition: "created" as const,
      },
      terminalState: error === undefined
        ? { type: "cancelled" as const, ...(cancelledError === undefined ? {} : { error: cancelledError }) }
        : { type: "error" as const, error },
    };
  });
  return {
    sessionThreadId: input.sessionThreadId,
    operationKind: "commit_inputs",
    sourceKind: input.sourceKind,
    operationId: input.runtimeInputId,
    declarationDigest: input.declarationDigest,
    events,
    messages,
    pendingAttachmentDelta: parsePendingAttachments(input.pendingAttachmentJson.map((item) => JSON.parse(item))) ?? [],
    interruptToolProjections,
    prefixConsumptions: [],
    childLifecycle: [],
  };
}

/** Builds the hot projection receipt for the dedicated task-notification commit operation. */
function hotTaskNotificationReceiptFromAssignedContext(input: {
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly runtimeInputId: string;
  readonly taskId: string;
  readonly declarationDigest: string;
  readonly create: CoreRuntimeMessageCreate;
  readonly assignedContextSequences: readonly number[];
}): RuntimeDeclarationReceipt {
  const receipt = hotReceiptFromAssignedContext({
    sessionId: input.sessionId,
    sessionThreadId: input.sessionThreadId,
    sourceKind: "task_notification",
    runtimeInputId: input.runtimeInputId,
    declarationDigest: input.declarationDigest,
    creates: [input.create],
    assignedContextSequences: input.assignedContextSequences,
    interruptToolResults: [],
    pendingAttachmentJson: [],
  });
  return {
    ...receipt,
    operationKind: "commit_task_notification_result",
    operationId: taskNotificationOperationId(input.runtimeInputId, input.taskId),
  };
}

function hotContextIdentity(kind: string, runtimeInputId: string, ...positions: number[]): string {
  return `hot_${createHash("sha256").update([kind, runtimeInputId, ...positions].join("\u0000")).digest("hex").slice(0, 24)}`;
}

function orderedText(creates: readonly CoreRuntimeMessageCreate[]): string[] {
  return creates.flatMap((create) => create.parts.flatMap((part) => part.type === "text" ? [part.text] : []));
}

function exactlyOneDefined(...values: readonly unknown[]): boolean {
  return values.filter((value) => value !== undefined).length === 1;
}

function bridgeCommitErrorRetryable(error: unknown): boolean {
  const code = typeof error === "object" && error !== null && "code" in error ? (error as { readonly code?: unknown }).code : undefined;
  return code !== status.INVALID_ARGUMENT && code !== status.ALREADY_EXISTS;
}

function bridgeDeclarationTransportUnknown(error: unknown): boolean {
  const code = typeof error === "object" && error !== null && "code" in error ? (error as { readonly code?: unknown }).code : undefined;
  return code === status.UNAVAILABLE ||
    code === status.DEADLINE_EXCEEDED ||
    code === status.INTERNAL ||
    code === status.UNKNOWN;
}

function bridgeStoreErrorCode(error: unknown): "unavailable" | "constraint_violation" {
  const code = typeof error === "object" && error !== null && "code" in error ? (error as { readonly code?: unknown }).code : undefined;
  return code === status.ALREADY_EXISTS ? "constraint_violation" : "unavailable";
}

function internalToolRepairStoreFailure(
  code: "unavailable" | "constraint_violation",
  sessionId: string,
  constraint?: string | undefined,
): RuntimeInternalToolRepairCommitResult {
  return {
    ok: false,
    error: normalizeRuntimeMessageStoreError({
      code,
      operation: "commitInternalToolRepair",
      reason: "runtime_contract_validation",
      ...(constraint !== undefined && constraint !== "" ? { constraint } : {}),
      sessionId,
    }),
  };
}

function parseContextPayload(contextJson: string, input: RuntimeThreadAddressState, logger?: RuntimePodLogger | undefined): {
  readonly messages: readonly DurableRuntimeMessage[];
  readonly turnFacts: ThreadTurnLoadFacts;
  readonly threadContextPrefix?: ThreadContextPrefix | undefined;
  readonly durableTurnId?: string | undefined;
  readonly thread: RuntimeAcceptedThreadMetadataState;
  readonly runtimeConfigPatch?: RuntimeConfigPatchState | undefined;
  readonly mcpManifests?: readonly RuntimeConfigPatchState[] | undefined;
  readonly pendingToolUses?: readonly RuntimeLoadedPendingToolUse[] | undefined;
  readonly pendingSandboxExecutions?: readonly RuntimePreloadedSandboxExecutionState[] | undefined;
  readonly backgroundTools?: readonly RuntimePreloadedBackgroundToolState[] | undefined;
  readonly pendingAttachments?: readonly RuntimeProviderAttachment[] | undefined;
  readonly pendingAgentMail?: readonly RuntimeLoadedAgentMail[] | undefined;
  readonly coldCoverage: RuntimeColdCoverage;
} {
  try {
    const parsed = parseContextLoadPhase(logger, input, "context_json_parse", "invalid_context_json", () => {
      const value: unknown = JSON.parse(contextJson);
      if (!isRecord(value)) {
        throw new Error("load context JSON is malformed");
      }
      return value;
    });
    const messageValues = parseContextLoadPhase(
      logger,
      input,
      "message_collection_parse",
      "invalid_message_collection_shape",
      () => {
        if (!Array.isArray(parsed.messages)) {
          throw new Error("load context messages are malformed");
        }
        return parsed.messages;
      },
    );
    const messages = parseContextLoadPhase(
      logger,
      input,
      "durable_message_parse",
      () => durableMessageParseFailureReason(messageValues),
      () => messageValues.map((message) => DurableRuntimeMessageSchema.parse(message)),
    );
    const turnFacts = parseContextLoadPhase(
      logger,
      input,
      "turn_facts_parse",
      "invalid_turn_facts_shape",
      () => ThreadTurnLoadFactsSchema.parse(parsed.turnFacts),
    );
    const threadContextPrefix = parseContextLoadPhase(
      logger,
      input,
      "thread_context_prefix_parse",
      "invalid_thread_context_prefix_shape",
      () => parseThreadContextPrefix(parsed.threadContextPrefix),
    );
    const durableTurnId = stringField(parsed, "durableTurnId");
    const thread = parseContextLoadPhase(
      logger,
      input,
      "thread_metadata_parse",
      "invalid_thread_metadata_shape",
      () => parseThreadMetadata(parsed.thread),
    );
    const runtimeConfigPatch = parseContextLoadPhase(
      logger,
      input,
      "runtime_config_parse",
      "invalid_runtime_config_shape",
      () => runtimeConfigPatchFromContextPayload(parsed, input),
    );
    const mcpManifests = parseContextLoadPhase(
      logger,
      input,
      "mcp_manifests_parse",
      "invalid_mcp_manifests_shape",
      () => mcpManifestPatchesFromContextPayload(parsed.mcpManifests, input),
    );
    const pendingToolUses = parseContextLoadPhase(
      logger,
      input,
      "pending_tool_uses_parse",
      "invalid_pending_tool_uses_shape",
      () => parsePendingToolUses(parsed.pendingToolUses),
    );
    const pendingSandboxExecutions = parseContextLoadPhase(
      logger,
      input,
      "pending_sandbox_executions_parse",
      "invalid_pending_sandbox_executions_shape",
      () => parsePendingSandboxExecutions(parsed.pendingSandboxExecutions),
    );
    const backgroundTools = parseContextLoadPhase(
      logger,
      input,
      "background_tools_parse",
      "invalid_background_tools_shape",
      () => parseBackgroundTools(parsed.backgroundTools),
    );
    const pendingAttachments = parseContextLoadPhase(
      logger,
      input,
      "pending_attachments_parse",
      "invalid_pending_attachments_shape",
      () => parsePendingAttachments(parsed.pendingAttachments),
    );
    const pendingAgentMail = parseContextLoadPhase(
      logger,
      input,
      "pending_agent_mail_parse",
      "invalid_pending_agent_mail_shape",
      () => parsePendingAgentMail(parsed.pendingAgentMail, input.sessionId),
    );
    const coldCoverage = parseContextLoadPhase(
      logger,
      input,
      "cold_coverage_parse",
      "invalid_cold_coverage_shape",
      () => parseColdCoverage(parsed.coldCoverage),
    );
    return {
      messages,
      turnFacts,
      ...(threadContextPrefix !== undefined ? { threadContextPrefix } : {}),
      ...(durableTurnId !== undefined ? { durableTurnId } : {}),
      thread,
      ...(runtimeConfigPatch !== undefined ? { runtimeConfigPatch } : {}),
      ...(mcpManifests !== undefined ? { mcpManifests } : {}),
      ...(pendingToolUses !== undefined ? { pendingToolUses } : {}),
      ...(pendingSandboxExecutions !== undefined ? { pendingSandboxExecutions } : {}),
      ...(backgroundTools !== undefined ? { backgroundTools } : {}),
      ...(pendingAttachments !== undefined ? { pendingAttachments } : {}),
      ...(pendingAgentMail !== undefined ? { pendingAgentMail } : {}),
      coldCoverage,
    };
  } catch (error) {
    throw normalizeContextLoaderError({
      code: "schema_mismatch",
      rawError: error,
      sessionId: input.sessionId,
      reason: "load context returned malformed RuntimeMessage projection",
    });
  }
}

function parseContextLoadPhase<T>(
  logger: RuntimePodLogger | undefined,
  input: RuntimeThreadAddressState,
  phase: RuntimeContextLoadParsePhase,
  reason: RuntimeContextLoadParseReason | (() => RuntimeContextLoadParseReason),
  parse: () => T,
): T {
  try {
    return parse();
  } catch (error) {
    recordContextLoadParseFailure(
      logger,
      input,
      phase,
      typeof reason === "function" ? reason() : reason,
    );
    throw error;
  }
}

function durableMessageParseFailureReason(
  messages: readonly unknown[],
): "invalid_tool_error_shape" | "invalid_durable_message_shape" {
  for (const message of messages) {
    if (!isRecord(message) || !Array.isArray(message.parts)) continue;
    for (const part of message.parts) {
      if (!isRecord(part) || part.type !== "tool" || !isRecord(part.state)) continue;
      const state = part.state;
      const hasError = Object.prototype.hasOwnProperty.call(state, "error");
      if ((state.status === "error" || (state.status === "cancelled" && hasError)) &&
        !RuntimeToolErrorSchema.safeParse(state.error).success) {
        return "invalid_tool_error_shape";
      }
    }
  }
  return "invalid_durable_message_shape";
}

function parseColdCoverage(value: unknown): RuntimeColdCoverage {
  if (!isRecord(value)) {
    throw new Error("load context coldCoverage is malformed");
  }
  const stringList = (field: string): readonly string[] => {
    const entries = value[field];
    if (
      !Array.isArray(entries) ||
      entries.some((entry) => typeof entry !== "string" || entry.length === 0) ||
      new Set(entries).size !== entries.length
    ) {
      throw new Error("load context coldCoverage is malformed");
    }
    return entries;
  };
  return {
    pendingToolIds: stringList("pendingToolIds"),
    pendingSandboxExecutionIds: stringList("pendingSandboxExecutionIds"),
    pendingAttachmentIdentities: stringList("pendingAttachmentIdentities"),
    undeliveredMailDeliveryIds: stringList("undeliveredMailDeliveryIds"),
  };
}

function parsePendingSandboxExecutions(value: unknown): readonly RuntimePreloadedSandboxExecutionState[] | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (!Array.isArray(value)) {
    throw new Error("load context pendingSandboxExecutions is malformed");
  }
  const allowedStates = new Set<RuntimePreloadedSandboxExecutionState["executionState"]>([
    "pending",
    "preparing",
    "running",
    "waiting_activation",
    "waiting_materialization",
    "terminal_unconsumed",
  ]);
  return value.map((item): RuntimePreloadedSandboxExecutionState => {
    if (!isRecord(item)) {
      throw new Error("load context pendingSandboxExecutions is malformed");
    }
    const executionState = requiredStringField(item, "executionState") as RuntimePreloadedSandboxExecutionState["executionState"];
    if (!allowedStates.has(executionState)) {
      throw new Error("load context pendingSandboxExecutions is malformed");
    }
    return {
      toolUseEventId: requiredStringField(item, "toolUseEventId"),
      modelRequestId: requiredStringField(item, "modelRequestId"),
      modelToolCallId: requiredStringField(item, "modelToolCallId"),
      toolName: requiredStringField(item, "toolName"),
      input: RuntimeJsonValueSchema.parse(item.input ?? {}),
      executionState,
    };
  });
}

function parseThreadContextPrefix(value: unknown): ThreadContextPrefix | undefined {
  if (value === undefined || value === null) {
    return undefined;
  }
  if (!isRecord(value) || !Array.isArray(value.entries)) {
    throw new Error("load context threadContextPrefix is malformed");
  }
  const consumed = stringField(value, "consumedByCheckpointMessageId");
  return {
    childThreadId: requiredStringField(value, "childThreadId"),
    parentThreadId: requiredStringField(value, "parentThreadId"),
    parentBoundaryEventId: requiredStringField(value, "parentBoundaryEventId"),
    entries: value.entries.map((entry) => RuntimeMessageSchema.parse(entry)),
    createdAt: requiredStringField(value, "createdAt"),
    ...(consumed !== undefined ? { consumedByCheckpointMessageId: consumed } : {}),
  };
}

function parseThreadMetadata(value: unknown): RuntimeAcceptedThreadMetadataState {
  if (!isRecord(value)) {
    throw new Error("load context thread is malformed");
  }
  const role = stringField(value, "role");
  const visibility = stringField(value, "visibility");
  const status = stringField(value, "status");
  const agentType = stringField(value, "agentType");
  if (
    (role !== "main" && role !== "subagent" && role !== "approval_reviewer") ||
    (visibility !== "public" && visibility !== "internal") ||
    !["idle", "running", "requires_action", "closed_for_runtime", "rescheduling", "terminated", "failed"].includes(status ?? "") ||
    agentType === undefined
  ) {
    throw new Error("load context thread is malformed");
  }
  const parentThreadId = stringField(value, "parentThreadId");
  const parentTaskName = stringField(value, "parentTaskName");
  const taskName = stringField(value, "taskName");
  return {
    role,
    visibility,
    status: status as RuntimeAcceptedThreadMetadataState["status"],
    agentType: agentType as NonNullable<RuntimeAcceptedThreadMetadataState["agentType"]>,
    ...(parentThreadId !== undefined ? { parentThreadId } : {}),
    ...(parentTaskName !== undefined ? { parentTaskName } : {}),
    ...(taskName !== undefined ? { taskName } : {}),
  };
}

function parsePendingAgentMail(value: unknown, sessionId: string): readonly RuntimeLoadedAgentMail[] | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (!Array.isArray(value)) {
    throw new Error("load context pendingAgentMail is malformed");
  }
  const parsed = value.map((item): RuntimeLoadedAgentMail => {
    if (!isRecord(item)) {
      throw new Error("load context pendingAgentMail is malformed");
    }
    const deliveryId = requiredStringField(item, "deliveryId");
    const content = requiredStringField(item, "content");
    return {
      deliveryId,
      message: runtimeMessageFromAgentMailContent({
        sessionId,
        deliveryId,
        content,
      }),
      content,
    };
  });
  if (parsed.length > MailFetchMaxEnvelopes) {
    throw new Error("load context pendingAgentMail exceeds the envelope bound");
  }
  return parsed;
}

function parsePendingAttachments(value: unknown): readonly RuntimeProviderAttachment[] | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (!Array.isArray(value)) {
    throw new Error("load context pendingAttachments is malformed");
  }
  return value.map((item): RuntimeProviderAttachment => {
    if (!isRecord(item) || !isRecord(item.origin)) {
      throw new Error("load context pendingAttachments is malformed");
    }
    const transient = recordField(item.origin, "transient");
    const fileBacked = recordField(item.origin, "fileBacked") ?? recordField(item.origin, "file_backed");
    if ((transient === undefined) === (fileBacked === undefined)) {
      throw new Error("load context pendingAttachments is malformed");
    }
    const mime = requiredStringField(item, "mime");
    const filename = stringField(item, "filename");
    if (filename === undefined) {
      throw new Error("load context pendingAttachments is malformed");
    }
    if (transient !== undefined) {
      if (!["image/png", "image/jpeg", "image/gif", "image/webp", "application/pdf"].includes(mime)) {
        throw new Error("load context pendingAttachments is malformed");
      }
      return {
        transient: {
          attachmentRef: requiredStringField(transient, "attachmentRef"),
          sourceToolUseEventId: requiredStringField(transient, "sourceToolUseEventId"),
          sourcePath: stringField(transient, "sourcePath") ?? "",
          pageRange: stringField(transient, "pageRange") ?? "",
          detail: stringField(transient, "detail") ?? "",
        },
        fileBacked: undefined,
        mime,
        filename,
      };
    }
    if (!["image/png", "image/jpeg", "image/gif", "image/webp", "application/pdf", "text/plain"].includes(mime)) {
      throw new Error("load context pendingAttachments is malformed");
    }
    return {
      transient: undefined,
      fileBacked: {
        sourceEventId: requiredStringField(fileBacked!, "sourceEventId"),
        fileId: requiredStringField(fileBacked!, "fileId"),
      },
      mime,
      filename,
    };
  });
}

function mcpManifestPatchesFromContextPayload(
  value: unknown,
  input: RuntimeThreadAddressState,
): readonly RuntimeConfigPatchState[] | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (!Array.isArray(value)) {
    throw new Error("load context mcpManifests is malformed");
  }
  return value.map((item): RuntimeConfigPatchState => {
    if (!isRecord(item)) {
      throw new Error("load context mcpManifests is malformed");
    }
    const mcpServerName = stringField(item, "mcpServerName") ?? stringField(item, "mcp_server_name");
    const manifestETag = stringField(item, "manifestETag") ?? stringField(item, "manifest_etag");
    const generation = numberField(item, "manifestGeneration") ?? numberField(item, "manifest_generation");
    const readiness = stringField(item, "readiness") ?? "ready";
    const diagnostic = stringField(item, "diagnostic");
    const readinessShape = item.readiness === undefined || typeof item.readiness === "string";
    const diagnosticShape = item.diagnostic === undefined || item.diagnostic === null || typeof item.diagnostic === "string";
    const etagAbsent = item.manifestETag === undefined && item.manifest_etag === undefined;
    const ready = readiness === "ready" && manifestETag !== undefined && diagnostic === undefined && diagnosticShape && Array.isArray(item.tools);
    const unready = readiness === "unready" && etagAbsent && diagnostic !== undefined && diagnosticShape &&
      Array.isArray(item.tools) && item.tools.length === 0;
    if (mcpServerName === undefined || generation === undefined ||
      !Number.isSafeInteger(generation) || generation <= 0 || !readinessShape || (!ready && !unready)) {
      throw new Error("load context mcpManifests is malformed");
    }
    const manifestReadiness = readiness === "unready" ? "unready" : "ready";
    const tools = (item.tools as readonly unknown[]).map((tool): Record<string, unknown> => {
      if (!isRecord(tool)) {
        throw new Error("load context mcpManifests is malformed");
      }
      const name = requiredStringField(tool, "name");
      const description = stringField(tool, "description");
      const inputSchema = tool.inputSchema ?? tool.input_schema;
      if (description === undefined || !isRecord(inputSchema)) {
        throw new Error("load context mcpManifests is malformed");
      }
      return { name, description, input_schema: inputSchema };
    });
    return {
      ...input,
      configIdentity: `mcp_manifest:${mcpServerName}`,
      generation,
      mcpServerName,
      ...(manifestETag !== undefined ? { manifestETag } : {}),
      manifestReadiness,
      ...(diagnostic !== undefined ? { manifestDiagnostic: diagnostic } : {}),
      contentJson: JSON.stringify({
        mcp_manifest: {
          mcp_server_name: mcpServerName,
          manifest_generation: generation,
          readiness: manifestReadiness,
          diagnostic: diagnostic ?? null,
          ...(manifestETag !== undefined ? { manifest_etag: manifestETag } : {}),
          ...(manifestReadiness === "ready" ? { tools } : {}),
        },
      }),
    };
  });
}

function parseBackgroundTools(value: unknown): readonly RuntimePreloadedBackgroundToolState[] | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (!Array.isArray(value)) {
    throw new Error("load context backgroundTools is malformed");
  }
  return value.map((item): RuntimePreloadedBackgroundToolState => {
    if (!isRecord(item)) {
      throw new Error("load context backgroundTools is malformed");
    }
    return {
      taskId: requiredStringField(item, "taskId"),
      sourceToolUseEventId: requiredStringField(item, "sourceToolUseEventId"),
    };
  });
}

function runtimeConfigPatchFromContextPayload(
  payload: Record<string, unknown>,
  input: RuntimeThreadAddressState,
): RuntimeConfigPatchState | undefined {
  const runtimeConfig = recordField(payload, "runtimeConfig") ?? recordField(payload, "runtime_config");
  if (runtimeConfig === undefined) {
    return undefined;
  }
  if (!isRecord(runtimeConfig)) {
    throw new Error("load context runtimeConfig is malformed");
  }
  const generation = numberField(runtimeConfig, "configGeneration") ?? numberField(runtimeConfig, "config_generation");
  if (generation === undefined || !Number.isSafeInteger(generation) || generation <= 0) {
    throw new Error("load context runtimeConfig generation is malformed");
  }
  const approvalMode = stringField(runtimeConfig, "approvalMode") ?? stringField(runtimeConfig, "approval_mode");
  const toolPolicy = recordField(runtimeConfig, "toolPolicy") ?? recordField(runtimeConfig, "tool_policy");
  const installedBuiltinFamily = installedBuiltinFamilyFromRuntimeConfig(runtimeConfig);
  return {
    ...input,
    configIdentity: "runtime_config",
    generation,
    coldLoad: true,
    installedBuiltinFamily,
    contentJson: JSON.stringify({
      config_generation: generation,
      ...(approvalMode !== undefined ? { approval_mode: approvalMode } : {}),
      ...(toolPolicy !== undefined ? { tool_policy: toolPolicy } : {}),
      runtime_config: runtimeConfig,
      ...(payload.pendingToolUses !== undefined ? { pending_tool_uses: payload.pendingToolUses } : {}),
    }),
  };
}

function installedBuiltinFamilyFromRuntimeConfig(runtimeConfig: Record<string, unknown>): "claude" | "gpt" {
  const tools = runtimeConfig.installedTools ?? runtimeConfig.installed_tools;
  if (!Array.isArray(tools)) {
    throw new Error("load context installed builtin family is malformed");
  }
  let family: "claude" | "gpt" | undefined;
  for (const tool of tools) {
    if (!isRecord(tool) || tool.type !== "tetral_agent_toolset") {
      continue;
    }
    if (family !== undefined || (tool.family !== "claude" && tool.family !== "gpt")) {
      throw new Error("load context installed builtin family is malformed");
    }
    family = tool.family;
  }
  if (family === undefined) {
    throw new Error("load context installed builtin family is malformed");
  }
  return family;
}

function parsePendingToolUses(value: unknown): readonly RuntimeLoadedPendingToolUse[] | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (!Array.isArray(value)) {
    throw new Error("load context pendingToolUses is malformed");
  }
  return value.map((item): RuntimeLoadedPendingToolUse => {
    if (!isRecord(item)) {
      throw new Error("load context pendingToolUses is malformed");
    }
    const status = stringField(item, "status");
    if (status !== "pending" && status !== "resolving") {
      throw new Error("load context pendingToolUses is malformed");
    }
    const decision = pendingToolDecision(item);
    const denyMessage = stringField(item, "denyMessage");
    return {
      toolUseEventId: requiredStringField(item, "toolUseEventId"),
      modelRequestId: requiredStringField(item, "modelRequestId"),
      modelToolCallId: requiredStringField(item, "modelToolCallId"),
      toolName: requiredStringField(item, "toolName"),
      input: RuntimeJsonValueSchema.parse(item.input ?? {}),
      ...(decision !== undefined ? { decision } : {}),
      ...(denyMessage !== undefined ? { denyMessage } : {}),
      status,
    };
  });
}

function pendingToolDecision(item: Record<string, unknown>): "allow" | "deny" | undefined {
  const decision = stringField(item, "decision");
  if (decision === undefined) {
    return undefined;
  }
  if (decision !== "allow" && decision !== "deny") {
    throw new Error("load context pendingToolUses is malformed");
  }
  return decision;
}

function recordField(value: Record<string, unknown>, field: string): Record<string, unknown> | undefined {
  const candidate = value[field];
  return isRecord(candidate) ? candidate : undefined;
}

function requiredStringField(value: Record<string, unknown>, field: string): string {
  const candidate = stringField(value, field);
  if (candidate === undefined || candidate.length === 0) {
    throw new Error(`load context ${field} is malformed`);
  }
  return candidate;
}

function stringField(value: Record<string, unknown>, field: string): string | undefined {
  const candidate = value[field];
  return typeof candidate === "string" ? candidate : undefined;
}

function numberField(value: Record<string, unknown>, field: string): number | undefined {
  const candidate = value[field];
  return typeof candidate === "number" ? candidate : undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function modelRequestIdForEvent(event: SessionEventEnvelope["event"]): string {
  switch (event.type) {
    case "span.model_request_start":
      return event.model_request_id;
    case "span.model_request_end":
      return event.model_request_start_id;
    default:
      return "";
  }
}

function eventWriterRejected(
  sessionId: string,
  writeId: string,
  errorCode: string | undefined,
  deterministic = false,
): SessionEventWriterAppendResult {
  const code =
    errorCode === "scope_superseded"
      ? "superseded"
      : errorCode === "closeout_unrepairable"
        ? "unrepairable"
        : deterministic
          ? "ack_mismatch"
          : "unavailable";
  return {
    ok: false,
    error: normalizeSessionEventWriterError({ code, sessionId, writeId }),
  };
}

function eventWriterSchemaFailure(
  sessionId: string,
  writeId: string,
): SessionEventWriterAppendResult {
  return {
    ok: false,
    error: normalizeSessionEventWriterError({
      code: "schema_mismatch",
      sessionId,
      writeId,
    }),
  };
}

class RuntimeTerminationResultContractError extends Error {}

function parseRuntimeTerminationResult(
  response: CommitRuntimeTerminationResponse,
): SessionEventWriterRuntimeTerminationResult {
  const variants = [response.committed, response.duplicate, response.stale]
    .filter((candidate) => candidate !== undefined).length;
  if (variants !== 1) {
    throw new RuntimeTerminationResultContractError("CommitRuntimeTermination returned an invalid result variant");
  }
  if (response.stale !== undefined) return { ok: true, type: "stale" };
  const terminal = response.committed ?? response.duplicate;
  if (terminal === undefined || terminal.failureEventId.length === 0 || terminal.closeoutEventId.length === 0) {
    throw new RuntimeTerminationResultContractError("CommitRuntimeTermination returned incomplete durable facts");
  }
  return {
    ok: true,
    type: response.committed === undefined ? "duplicate" : "committed",
    failureEventId: terminal.failureEventId,
    closeoutEventId: terminal.closeoutEventId,
  };
}

function parseToolSettlementResponse(
  response: SettleToolResultResponse,
  envelope: SessionEventWriterToolSettlementEnvelope,
): SessionEventWriterToolSettlementAttempt {
  const variants = [response.committed, response.duplicate, response.stale]
    .filter((candidate) => candidate !== undefined).length;
  if (variants !== 1) {
    return {
      ok: false,
      error: normalizeSessionEventWriterError({
        code: "schema_mismatch",
        sessionId: envelope.sessionId,
        writeId: envelope.settlement.toolUseEventId,
      }),
    };
  }
  if (response.committed !== undefined) return { ok: true, result: { type: "committed" } };
  if (response.duplicate !== undefined) return { ok: true, result: { type: "duplicate" } };
  return { ok: true, result: { type: "stale" } };
}

function toolSettlementTransportFailure(
  envelope: SessionEventWriterToolSettlementEnvelope,
  error: unknown,
): SessionEventWriterToolSettlementAttempt {
  const failure = eventWriterTransportFailure(
    envelope.sessionId,
    envelope.settlement.toolUseEventId,
    error,
    true,
  );
  return failure.ok
    ? { ok: false, error: normalizeSessionEventWriterError({ code: "unknown", sessionId: envelope.sessionId }) }
    : { ok: false, error: failure.error };
}

function eventWriterTransportFailure(
  sessionId: string,
  writeId: string,
  error: unknown,
  retryUnknown = false,
): SessionEventWriterAppendResult {
  const grpcCode =
    typeof error === "object" && error !== null && "code" in error && typeof error.code === "number"
      ? error.code
      : undefined;
  const code =
    grpcCode === status.DEADLINE_EXCEEDED
      ? "timeout"
      : grpcCode === status.UNAVAILABLE || grpcCode === status.ABORTED || grpcCode === status.RESOURCE_EXHAUSTED
        ? "unavailable"
        : retryUnknown && grpcCode === status.UNKNOWN
          ? "unavailable"
        : "unknown";
  return {
    ok: false,
    error: normalizeSessionEventWriterError({ code, rawError: error, sessionId, writeId }),
  };
}
