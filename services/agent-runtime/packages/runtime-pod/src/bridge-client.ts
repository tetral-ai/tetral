/**
 * @packageDocumentation
 * Adapts Runtime Core persistence and lifecycle ports to authenticated Agent Runtime Bridge
 * gRPC calls. Runtime Pod composition constructs these adapters, and Runtime Core uses them to
 * load cold thread state, commit inputs and events, manage reviewer threads, repair internal tool
 * state, and refresh binding tokens. The adapters call the generated Bridge client and outbound
 * metadata factory, accept committed or duplicate acknowledgements as durable proof, validate
 * populated projected JSON before exposing it, and keep per-thread scope bookkeeping local and disposable.
 */
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
  RuntimeDraftKind,
  SealedAgentMailDisposition,
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
  CreateChildThreadRequest,
  CreateChildThreadResponse,
  FinishIdleRequest,
  FinishIdleResponse,
  LoadContextRequest,
  LoadContextResponse,
  MarkChildThreadActiveResponse,
  MarkChildThreadClosedRequest,
  MarkChildThreadClosedResponse,
  RefreshRuntimeBindingTokenRequest,
  RefreshRuntimeBindingTokenResponse,
  ResolveInterAgentDeliveryRequest,
  ResolveInterAgentDeliveryResponse,
  RuntimeScope,
  WriteEventRequest,
  WriteEventResponse,
  WriteRequestEndRequest,
  WriteRequestEndResponse,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type { ProviderRequestAttachment } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { AcceptedInputCommitResult, ContextLoader, RuntimeLoadedAgentMail, RuntimeLoadedPendingToolUse, RuntimeResolvedAgentMail } from "@tetral/agent-runtime-core/src/context/context-loader.js";
import type {
  RuntimeAcceptedInputState,
  RuntimeAcceptedThreadMetadataState,
  RuntimeConfigPatchState,
  RuntimePreloadedBackgroundToolState,
  RuntimeThreadControlState,
  RuntimeColdCoverage,
} from "@tetral/agent-runtime-core/src/session/session-state.js";
import type { RuntimeSessionIdentity } from "@tetral/agent-runtime-core/src/session/session.js";
import type { ThreadContextPrefix } from "@tetral/agent-runtime-core/src/session/context-manager.js";
import {
  DurableRuntimeMessageSchema,
  RuntimeMessageSchema,
  RuntimeJsonValueSchema,
  SessionEventWriterRetryPolicy,
  normalizeContextLoaderError,
  normalizeRuntimeMessageStoreError,
  normalizeSessionEventWriterError,
} from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import { MailFetchMaxEnvelopes } from "@tetral/agent-runtime-protocol/src/bounds.js";
import type {
  RuntimeMessageDraft as CoreRuntimeMessageDraft,
  RuntimeInternalToolRepairCommit,
  RuntimeInternalToolRepairCommitResult,
  RuntimeMessage,
  SessionEventEnvelope,
  SessionEventWriter,
  SessionEventWriterAppendResult,
  SessionEventWriterFinishIdleEnvelope,
  SessionEventWriterRequestEndEnvelope,
  SessionEventWriterRuntimeTerminationEnvelope,
} from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import {
  acceptedInputDeclarationKind,
  applyTaskNotificationReceipt,
  taskNotificationSourceId,
} from "@tetral/agent-runtime-core/src/runtime/runtime-declaration.js";
import type { RuntimeDeclarationReceipt } from "@tetral/agent-runtime-core/src/runtime/runtime-declaration.js";
import { buildOutboundBearerMetadata } from "./auth.js";
import type { ServiceAccountTokenConfig } from "./auth.js";
import type { ApprovalReviewerThreadCreation, RuntimeApprovalReviewerThreadCreator } from "./approval-reviewer.js";
import type { RuntimeControlInputCommitter, RuntimeTaskNotificationCommitter } from "./runtime-service.js";
import {
  recordRuntimeReceiptEvidence,
} from "./logger.js";
import type {
  RuntimePodLogger,
  RuntimeReceiptEvidence,
  RuntimeReceiptEvidenceMetrics,
} from "./logger.js";
import { bridgeAttachmentGrpcChannelOptions, grpcClientChannelOptions } from "./bounds.js";
import { sessionEventForDurableWrite } from "@tetral/agent-runtime-core/src/runtime/session-event-writer.js";
import {
  childLifecycleDeclarationDigest,
  commitInputsDeclarationDigest,
  finishIdleDeclarationDigest,
  internalToolRepairDeclarationDigest,
  runtimeTerminationDeclarationDigest,
  taskNotificationDeclarationDigest,
  writeEventDeclarationDigest,
  writeRequestEndDeclarationDigest,
} from "./runtime-declaration-wire.js";
import { stableRuntimeID } from "@tetral/agent-runtime-core/src/runtime/runtime-identity.js";
import { runtimeMessageFromPublicAgentMail } from "./agent-mail.js";

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
    const request = {
      scope: bridgeScope(input.scope),
      runtimeInputId: input.scope.runtimeInputId,
      eventIds: [...input.scope.eventIds],
      sequenceFrom: input.scope.sequenceFrom,
      sequenceTo: input.scope.sequenceTo,
      inputKind: input.inputKind,
      drafts: input.drafts.map(runtimeMessageDraftForBridge),
      pendingToolCancellations: input.pendingToolCancellations.map((cancellation) => ({ ...cancellation })),
    };
    const declarationDigest = commitInputsDeclarationDigest(request);
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
    if (bridgeAckAccepted(response.ack?.status)) {
      const declaration = response.declaration;
      const bridgeReceipt = declaration?.receipts[0];
      const applicationDisposition = declaration === undefined
        ? undefined
        : runtimeReceiptApplicationDisposition(declaration.applicationDisposition);
      let receipt: RuntimeDeclarationReceipt | undefined;
      try {
        receipt = bridgeReceipt === undefined ? undefined : runtimeDeclarationReceipt(bridgeReceipt);
      } catch {
        receipt = undefined;
      }
      const draftByLocalId = new Map(input.drafts.map((draft) => [draft.runtimeLocalId, draft]));
      const receiptValid =
        declaration !== undefined &&
        declaration.receipts.length === 1 &&
        receipt !== undefined &&
        applicationDisposition !== undefined &&
        receipt.sessionThreadId === input.scope.sessionThreadId &&
        receipt.operationKind === "commit_inputs" &&
        receipt.sourceKind === input.inputKind &&
        receipt.sourceId === input.scope.runtimeInputId &&
        receipt.declarationDigest === declarationDigest &&
        draftByLocalId.size === input.drafts.length &&
        receipt.messages.length === input.drafts.length &&
        new Set(receipt.messages.map((message) => message.runtimeLocalId)).size === receipt.messages.length &&
        receipt.messages.every((message) => {
          const draft = draftByLocalId.get(message.runtimeLocalId);
          const draftPartIds = new Set(draft?.parts.map((part) => part.runtimeLocalPartId));
          return draft !== undefined &&
            message.sessionThreadId === input.scope.sessionThreadId &&
            message.owningEventId === draft.sourceEventId &&
            draftPartIds.size === draft.parts.length &&
            message.parts.length === draft.parts.length &&
            new Set(message.parts.map((part) => part.runtimeLocalPartId)).size === message.parts.length &&
            message.parts.every((part) =>
              draftPartIds.has(part.runtimeLocalPartId) &&
              part.messageId === message.messageId
            );
        }) &&
        receipt.pendingAttachmentDelta.length === 0 &&
        receipt.pendingToolDelta.length === input.pendingToolCancellations.length &&
        receipt.prefixConsumptions.length === 0 &&
        receipt.requestReschedule === undefined &&
        receipt.idleCloseout === undefined &&
        receipt.compactedThroughMessageSequence === undefined &&
        receipt.childLifecycle.length === 0 &&
        receipt.sealedAgentMail === undefined &&
        receipt.events.length === input.scope.eventIds.length &&
        receipt.events.every((event, index) =>
          event.sessionThreadId === input.scope.sessionThreadId &&
          event.sourceEventId === input.scope.eventIds[index] &&
          event.eventId === input.scope.eventIds[index]
        );
      if (!receiptValid) {
        recordBridgeReceiptEvidence(this.options, {
          workspaceId: input.scope.workspaceId,
          sessionId: input.scope.sessionId,
          sessionThreadId: input.scope.sessionThreadId,
          operation: "commit_inputs",
          sourceKind: input.inputKind,
          sourceId: input.scope.runtimeInputId,
          declarationDigest,
          bindingId: declaration?.observedBindingId ?? input.scope.bindingId,
          bindingGeneration: declaration?.observedBindingGeneration ?? input.scope.bindingGeneration,
          ...(applicationDisposition === undefined ? {} : { applicationDisposition }),
          outcome: receipt?.declarationDigest !== undefined && receipt.declarationDigest !== declarationDigest
            ? "declaration_digest_mismatch"
            : "receipt_shape_invalid",
        });
        return {
          ok: false as const,
          retryable: false,
          errorCode: "bridge_control_input_receipt_invalid",
          message: "control input durable commit returned malformed receipt",
        };
      }
      if (applicationDisposition === "stale_custody") {
        recordBridgeReceiptEvidence(this.options, {
          workspaceId: input.scope.workspaceId,
          sessionId: input.scope.sessionId,
          sessionThreadId: input.scope.sessionThreadId,
          operation: "commit_inputs",
          sourceKind: input.inputKind,
          sourceId: input.scope.runtimeInputId,
          declarationDigest,
          bindingId: declaration.observedBindingId,
          bindingGeneration: declaration.observedBindingGeneration,
          applicationDisposition,
          outcome: "stale_custody",
        });
        return { ok: true as const, stale: true as const };
      }
      if (
        declaration.observedBindingId !== input.scope.bindingId ||
        declaration.observedBindingGeneration !== input.scope.bindingGeneration
      ) {
        recordBridgeReceiptEvidence(this.options, {
          workspaceId: input.scope.workspaceId,
          sessionId: input.scope.sessionId,
          sessionThreadId: input.scope.sessionThreadId,
          operation: "commit_inputs",
          sourceKind: input.inputKind,
          sourceId: input.scope.runtimeInputId,
          declarationDigest,
          bindingId: declaration.observedBindingId,
          bindingGeneration: declaration.observedBindingGeneration,
          applicationDisposition,
          outcome: "binding_identity_mismatch",
        });
        return {
          ok: false as const,
          retryable: false,
          errorCode: "bridge_control_input_receipt_invalid",
          message: "control input durable commit returned mismatched custody",
        };
      }
      if (receipt === undefined) {
        return {
          ok: false as const,
          retryable: false,
          errorCode: "bridge_control_input_receipt_invalid",
          message: "control input durable commit returned malformed receipt",
        };
      }
      recordBridgeReceiptEvidence(this.options, {
        workspaceId: input.scope.workspaceId,
        sessionId: input.scope.sessionId,
        sessionThreadId: input.scope.sessionThreadId,
        operation: "commit_inputs",
        sourceKind: input.inputKind,
        sourceId: input.scope.runtimeInputId,
        declarationDigest,
        bindingId: declaration.observedBindingId,
        bindingGeneration: declaration.observedBindingGeneration,
        applicationDisposition,
        outcome: "applied",
      });
      return { ok: true as const, receipt };
    }
    return {
      ok: false as const,
      retryable: false,
      errorCode: response.ack?.errorCode || "bridge_commit_rejected",
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
export class BridgeAPITaskNotificationCommitter implements RuntimeTaskNotificationCommitter {
  private readonly client: AgentRuntimeBridgeServiceClient;
  private readonly metadataFactory: (config: ServiceAccountTokenConfig) => Promise<Metadata>;

  constructor(private readonly options: BridgeAPITaskNotificationCommitterOptions) {
    this.client = options.client ?? new AgentRuntimeBridgeServiceClient(options.address, credentials.createInsecure(), grpcClientChannelOptions());
    this.metadataFactory = options.metadataFactory ?? buildOutboundBearerMetadata;
  }

  /** Commits one task-notification draft and returns its receipt-stamped message or stale disposition. */
  async commitTaskNotification(input: Parameters<RuntimeTaskNotificationCommitter["commitTaskNotification"]>[0]) {
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
        requestId: input.scope.requestId,
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
      taskId: input.command.taskId,
      resultJson: input.command.payloadJson,
      draft: runtimeMessageDraftForBridge(input.command.draft),
    };
    const declarationDigest = taskNotificationDeclarationDigest(request);
    let response: CommitTaskNotificationResultResponse;
    try {
      response = await commitTaskNotificationResult(this.client, request, metadata);
    } catch {
      return {
        ok: false as const,
        retryable: true,
        errorCode: "bridge_commit_unavailable",
        message: "task notification durable commit failed",
      };
    }
    const status = response.ack?.status;
    if (status === BridgeWriteStatus.BRIDGE_WRITE_STATUS_COMMITTED || status === BridgeWriteStatus.BRIDGE_WRITE_STATUS_DUPLICATE) {
      const declaration = response.declaration;
      const bridgeReceipt = declaration?.receipts[0];
      const applicationDisposition = declaration === undefined
        ? undefined
        : runtimeReceiptApplicationDisposition(declaration.applicationDisposition);
      const sourceId = taskNotificationSourceId(input.command.runtimeInputId, input.command.taskId);
      if (
        declaration !== undefined &&
        declaration.receipts.length === 1 &&
        bridgeReceipt !== undefined &&
        applicationDisposition === "stale_custody" &&
        bridgeReceipt.declarationDigest === declarationDigest &&
        bridgeReceipt.compactedThroughMessageSequence === undefined
      ) {
        recordBridgeReceiptEvidence(this.options, {
          workspaceId: input.scope.workspaceId,
          sessionId: input.scope.sessionId,
          sessionThreadId: input.scope.sessionThreadId,
          operation: "commit_task_notification_result",
          sourceKind: "task_notification",
          sourceId,
          declarationDigest,
          bindingId: declaration.observedBindingId,
          bindingGeneration: declaration.observedBindingGeneration,
          applicationDisposition,
          outcome: "stale_custody",
        });
        return { ok: true as const, stale: true as const };
      }
      if (
        declaration === undefined ||
        declaration.receipts.length !== 1 ||
        bridgeReceipt === undefined ||
        applicationDisposition !== "current_custody" ||
        declaration.observedBindingId !== input.scope.bindingId ||
        declaration.observedBindingGeneration !== input.scope.bindingGeneration ||
        bridgeReceipt.declarationDigest !== declarationDigest
      ) {
        recordBridgeReceiptEvidence(this.options, {
          workspaceId: input.scope.workspaceId,
          sessionId: input.scope.sessionId,
          sessionThreadId: input.scope.sessionThreadId,
          operation: "commit_task_notification_result",
          sourceKind: "task_notification",
          sourceId,
          declarationDigest,
          bindingId: declaration?.observedBindingId ?? input.scope.bindingId,
          bindingGeneration: declaration?.observedBindingGeneration ?? input.scope.bindingGeneration,
          ...(applicationDisposition === undefined ? {} : { applicationDisposition }),
          outcome: bridgeReceipt !== undefined && bridgeReceipt.declarationDigest !== declarationDigest
            ? "declaration_digest_mismatch"
            : applicationDisposition === "current_custody" &&
                (
                  declaration?.observedBindingId !== input.scope.bindingId ||
                  declaration.observedBindingGeneration !== input.scope.bindingGeneration
                )
              ? "binding_identity_mismatch"
              : "receipt_shape_invalid",
        });
        return {
          ok: false as const,
          retryable: false,
          errorCode: "bridge_task_notification_projection_invalid",
          message: "task notification durable commit returned malformed receipt",
        };
      }
      try {
        const receipt = ordinaryRuntimeDeclarationReceipt(bridgeReceipt);
        const messages = applyTaskNotificationReceipt({
          sessionId: input.scope.sessionId,
          sessionThreadId: input.scope.sessionThreadId,
          sourceId: taskNotificationSourceId(input.command.runtimeInputId, input.command.taskId),
          draft: input.command.draft,
          existingMessages: [],
        }, receipt);
        if (messages.length !== 1 || messages[0] === undefined) {
          throw new Error("task notification receipt did not stamp exactly one message");
        }
        recordBridgeReceiptEvidence(this.options, {
          workspaceId: input.scope.workspaceId,
          sessionId: input.scope.sessionId,
          sessionThreadId: input.scope.sessionThreadId,
          operation: "commit_task_notification_result",
          sourceKind: "task_notification",
          sourceId,
          declarationDigest,
          bindingId: declaration.observedBindingId,
          bindingGeneration: declaration.observedBindingGeneration,
          applicationDisposition,
          outcome: "applied",
        });
        return { ok: true as const, committedMessage: messages[0] };
      } catch {
        recordBridgeReceiptEvidence(this.options, {
          workspaceId: input.scope.workspaceId,
          sessionId: input.scope.sessionId,
          sessionThreadId: input.scope.sessionThreadId,
          operation: "commit_task_notification_result",
          sourceKind: "task_notification",
          sourceId,
          declarationDigest,
          bindingId: declaration.observedBindingId,
          bindingGeneration: declaration.observedBindingGeneration,
          applicationDisposition,
          outcome: "receipt_application_failed",
        });
        return {
          ok: false as const,
          retryable: false,
          errorCode: "bridge_task_notification_projection_invalid",
          message: "task notification durable commit returned malformed receipt",
        };
      }
    }
    if (status === BridgeWriteStatus.BRIDGE_WRITE_STATUS_REJECTED && response.ack?.errorCode === "task_notification_stale") {
      return { ok: true as const, stale: true as const };
    }
    return {
      ok: false as const,
      retryable: false,
      errorCode: response.ack?.errorCode || "bridge_commit_rejected",
      message: "task notification durable commit rejected",
    };
  }
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
  private readonly pendingCloseTimestamps = new Map<string, string>();

  constructor(private readonly options: BridgeAPIApprovalReviewerThreadCreatorOptions) {
    this.client = options.client ?? new AgentRuntimeBridgeServiceClient(options.address, credentials.createInsecure(), grpcClientChannelOptions());
    this.metadataFactory = options.metadataFactory ?? buildOutboundBearerMetadata;
  }

  /** Creates a prefixless trunk or a sidecar whose thread-context-prefix snapshot may be empty, ACK-gated. */
  async createApprovalReviewerThread(input: ApprovalReviewerThreadCreation) {
    let metadata: Metadata;
    try {
      metadata = await this.metadataFactory({ tokenPath: this.options.tokenPath });
    } catch {
      return { ok: false as const, message: "approval reviewer thread credential is unavailable" };
    }
    let response: CreateChildThreadResponse;
    try {
      response = await createChildThread(this.client, approvalReviewerCreateChildThreadRequest(input), metadata);
    } catch {
      return { ok: false as const, message: "approval reviewer thread creation is unavailable" };
    }
    if (bridgeAckAccepted(response.ack?.status)) {
      return { ok: true as const };
    }
    return {
      ok: false as const,
      message: response.ack?.errorCode || "approval reviewer thread was not acknowledged",
    };
  }

  /** Closes the reviewer child thread and releases local scope only after Bridge acknowledges it. */
  async closeApprovalReviewerThread(input: ApprovalReviewerThreadCreation) {
    let metadata: Metadata;
    try {
      metadata = await this.metadataFactory({ tokenPath: this.options.tokenPath });
    } catch {
      return { ok: false as const, message: "approval reviewer thread credential is unavailable" };
    }
    let response: MarkChildThreadClosedResponse;
    const timestampKey = [
      input.request.workspaceId,
      input.request.sessionId,
      input.request.sessionThreadId,
      input.reviewerThreadId,
      input.reviewId,
    ].join("\u0000");
    const closedAt = this.pendingCloseTimestamps.get(timestampKey) ?? new Date().toISOString();
    this.pendingCloseTimestamps.set(timestampKey, closedAt);
    try {
      response = await markChildThreadClosed(this.client, {
        scope: approvalReviewerParentScope(input),
        childThreadId: input.reviewerThreadId,
        closedAt,
        source: { reviewerReviewId: input.reviewId },
      }, metadata);
    } catch {
      return { ok: false as const, message: "approval reviewer thread close is unavailable" };
    }
    const declaration = validateChildLifecycleDeclarationResponse({
      action: "close",
      sessionThreadId: input.request.sessionThreadId,
      childThreadId: input.reviewerThreadId,
      sourceKind: "approval_review",
      sourceCommandId: input.reviewId,
      requestedAt: closedAt,
      bindingId: input.request.bindingId,
      bindingGeneration: input.request.bindingGeneration,
    }, response);
    if (declaration.ok) {
      this.pendingCloseTimestamps.delete(timestampKey);
      return { ok: true as const };
    }
    if (!declaration.discardHotState) {
      this.pendingCloseTimestamps.delete(timestampKey);
    }
    return {
      ok: false as const,
      message: declaration.errorCode,
      discardHotState: declaration.discardHotState,
    };
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

  constructor(private readonly options: BridgeAPIContextLoaderOptions) {
    this.client = options.client ?? new AgentRuntimeBridgeServiceClient(options.address, credentials.createInsecure(), bridgeAttachmentGrpcChannelOptions());
    this.metadataFactory = options.metadataFactory ?? buildOutboundBearerMetadata;
    this.nowEpochMs = options.nowEpochMs ?? (() => Date.now());
    this.refreshMarginMs = options.refreshMarginMs ?? RuntimeBindingTokenRefreshPolicy.marginMs;
    this.sleep = options.sleep ?? (async (durationMs) => await new Promise<void>((resolve) => setTimeout(resolve, durationMs)));
  }

  /**
   * Returns a still-valid binding token or refreshes it through Bridge with bounded retries.
   * Concurrent refreshes for the same thread and binding share one in-flight request.
   */
  async refreshRuntimeBindingToken(
    identity: RuntimeSessionIdentity,
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

  private async refreshRuntimeBindingTokenOnce(identity: RuntimeSessionIdentity): Promise<string> {
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
    command: RuntimeThreadControlState,
  ): Promise<{
    readonly messages: readonly RuntimeMessage[];
    readonly threadContextPrefix?: ThreadContextPrefix | undefined;
    readonly durableTurnId?: string | undefined;
    readonly runtimeBindingToken: string;
    readonly thread: RuntimeAcceptedThreadMetadataState;
    readonly runtimeConfigPatch?: RuntimeConfigPatchState | undefined;
    readonly mcpManifests?: readonly RuntimeConfigPatchState[] | undefined;
    readonly pendingToolUses?: readonly RuntimeLoadedPendingToolUse[] | undefined;
    readonly backgroundTools?: readonly RuntimePreloadedBackgroundToolState[] | undefined;
    readonly pendingAttachments?: readonly ProviderRequestAttachment[] | undefined;
    readonly pendingAgentMail?: readonly RuntimeLoadedAgentMail[] | undefined;
    readonly coldCoverage: RuntimeColdCoverage;
  }> {
    return await this.loadContext(command);
  }

  /** Resolves and durably admits one stored inter-agent envelope. */
  async resolveAgentMail(
    command: RuntimeThreadControlState,
    childThreadId: string,
    deliveryId?: string,
  ): Promise<RuntimeResolvedAgentMail> {
    const metadata = await this.metadata();
    const response = await resolveInterAgentDelivery(this.client, {
      scope: bridgeScope(command),
      childThreadId,
      deliveryId: deliveryId ?? "",
    }, metadata);
    if (
      !bridgeAckAccepted(response.ack?.status) ||
      response.deliveryId.length === 0 ||
      response.sourceThreadId.length === 0 ||
      response.targetThreadId.length === 0 ||
      response.sourceToolUseEventId.length === 0 ||
      response.receivedEventId.length === 0 ||
      response.receivedSequence <= 0 ||
      response.messageJson.length === 0
    ) {
      throw normalizeContextLoaderError({
        code: "schema_mismatch",
        sessionId: command.sessionId,
        reason: response.ack?.errorCode || "agent mail resolver returned an invalid envelope",
      });
    }
    if (
      !(
        response.sourceThreadId === command.sessionThreadId &&
        response.targetThreadId === childThreadId
      ) &&
      !(
        response.sourceThreadId === childThreadId &&
        response.targetThreadId === command.sessionThreadId
      )
    ) {
      throw normalizeContextLoaderError({
        code: "schema_mismatch",
        sessionId: command.sessionId,
        reason: "agent mail resolver returned an invalid thread relationship",
      });
    }
    let message: RuntimeMessage;
    try {
      message = runtimeMessageFromPublicAgentMail(response.messageJson);
    } catch {
      throw normalizeContextLoaderError({
        code: "schema_mismatch",
        sessionId: command.sessionId,
        reason: "agent mail resolver returned an invalid public message",
      });
    }
    return {
      deliveryId: response.deliveryId,
      sourceThreadId: response.sourceThreadId,
      targetThreadId: response.targetThreadId,
      sourceToolUseEventId: response.sourceToolUseEventId,
      receivedEventId: response.receivedEventId,
      receivedSequence: response.receivedSequence,
      message,
      publicMessageJson: response.messageJson,
    };
  }

  /**
   * Commits accepted input and returns only the durable receipt disposition.
   * Cold installation is the sole context read during one thread residency.
   */
  async commitAcceptedInput(
    input: RuntimeAcceptedInputState,
    options?: {
      readonly drafts?: readonly CoreRuntimeMessageDraft[] | undefined;
    },
  ): Promise<AcceptedInputCommitResult> {
    const drafts = options?.drafts ?? [];
    const sourceKind = acceptedInputDeclarationKind(input);
    const metadata = await this.metadata();
    const scope = bridgeScope(input);
    const request = {
      scope,
      runtimeInputId: input.runtimeInputId,
      eventIds: [...input.eventIds],
      sequenceFrom: input.sequenceFrom,
      sequenceTo: input.sequenceTo,
      inputKind: sourceKind,
      drafts: drafts.map(runtimeMessageDraftForBridge),
      pendingToolCancellations: [],
    };
    const declarationDigest = commitInputsDeclarationDigest(request);
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
    if (!bridgeAckAccepted(response.ack?.status)) {
      throw normalizeContextLoaderError({
        code: "unavailable",
        sessionId: input.sessionId,
        reason: response.ack?.errorCode || "commit inputs rejected",
      });
    }
    const inputDisposition =
      response.ack?.status === BridgeWriteStatus.BRIDGE_WRITE_STATUS_DUPLICATE ? "duplicate" : "committed";
    const declaration = response.declaration;
    const receipt = declaration?.receipts[0];
    if (declaration === undefined || declaration.receipts.length !== 1 || receipt === undefined) {
      recordBridgeReceiptEvidence(this.options, {
        workspaceId: input.workspaceId,
        sessionId: input.sessionId,
        sessionThreadId: input.sessionThreadId,
        operation: "commit_inputs",
        sourceKind,
        sourceId: input.runtimeInputId,
        declarationDigest,
        bindingId: input.bindingId,
        bindingGeneration: input.bindingGeneration,
        outcome: "receipt_shape_invalid",
      });
      throw normalizeContextLoaderError({
        code: "schema_mismatch",
        sessionId: input.sessionId,
        reason: "commit inputs did not return exactly one declaration receipt",
      });
    }
    if (receipt.declarationDigest !== declarationDigest) {
      recordBridgeReceiptEvidence(this.options, {
        workspaceId: input.workspaceId,
        sessionId: input.sessionId,
        sessionThreadId: input.sessionThreadId,
        operation: "commit_inputs",
        sourceKind,
        sourceId: input.runtimeInputId,
        declarationDigest,
        bindingId: input.bindingId,
        bindingGeneration: input.bindingGeneration,
        outcome: "declaration_digest_mismatch",
      });
      throw normalizeContextLoaderError({
        code: "schema_mismatch",
        sessionId: input.sessionId,
        reason: "commit inputs returned a mismatched declaration digest",
      });
    }
    if (receipt.compactedThroughMessageSequence !== undefined) {
      throw normalizeContextLoaderError({
        code: "schema_mismatch",
        sessionId: input.sessionId,
        reason: "commit inputs returned a compaction boundary on an ordinary receipt",
      });
    }
    const applicationDisposition = declaration.applicationDisposition ===
      ReceiptApplicationDisposition.RECEIPT_APPLICATION_DISPOSITION_CURRENT_CUSTODY
      ? "current_custody"
      : declaration.applicationDisposition === ReceiptApplicationDisposition.RECEIPT_APPLICATION_DISPOSITION_STALE_CUSTODY
        ? "stale_custody"
        : undefined;
    if (applicationDisposition === undefined) {
      recordBridgeReceiptEvidence(this.options, {
        workspaceId: input.workspaceId,
        sessionId: input.sessionId,
        sessionThreadId: input.sessionThreadId,
        operation: "commit_inputs",
        sourceKind,
        sourceId: input.runtimeInputId,
        declarationDigest,
        bindingId: input.bindingId,
        bindingGeneration: input.bindingGeneration,
        outcome: "receipt_shape_invalid",
      });
      throw normalizeContextLoaderError({
        code: "schema_mismatch",
        sessionId: input.sessionId,
        reason: "commit inputs returned an invalid custody disposition",
      });
    }
    if (
      applicationDisposition === "current_custody" &&
      (
        declaration.observedBindingId !== input.bindingId ||
        declaration.observedBindingGeneration !== input.bindingGeneration
      )
    ) {
      recordBridgeReceiptEvidence(this.options, {
        workspaceId: input.workspaceId,
        sessionId: input.sessionId,
        sessionThreadId: input.sessionThreadId,
        operation: "commit_inputs",
        sourceKind,
        sourceId: input.runtimeInputId,
        declarationDigest,
        bindingId: declaration.observedBindingId,
        bindingGeneration: declaration.observedBindingGeneration,
        applicationDisposition,
        outcome: "binding_identity_mismatch",
      });
      throw normalizeContextLoaderError({
        code: "schema_mismatch",
        sessionId: input.sessionId,
        reason: "commit inputs returned mismatched current custody identity",
      });
    }
    recordBridgeReceiptEvidence(this.options, {
      workspaceId: input.workspaceId,
      sessionId: input.sessionId,
      sessionThreadId: input.sessionThreadId,
      operation: "commit_inputs",
      sourceKind,
      sourceId: input.runtimeInputId,
      declarationDigest,
      bindingId: declaration.observedBindingId,
      bindingGeneration: declaration.observedBindingGeneration,
      applicationDisposition,
      outcome: applicationDisposition === "current_custody" ? "applied" : "stale_custody",
    });
    return {
      type: "receipt",
      inputDisposition,
      applicationDisposition,
      receipt: runtimeDeclarationReceipt(receipt),
    };
  }

  private async loadContext(
    input: RuntimeThreadControlState,
  ): Promise<{
    readonly messages: readonly RuntimeMessage[];
    readonly threadContextPrefix?: ThreadContextPrefix | undefined;
    readonly durableTurnId?: string | undefined;
    readonly runtimeBindingToken: string;
    readonly thread: RuntimeAcceptedThreadMetadataState;
    readonly runtimeConfigPatch?: RuntimeConfigPatchState | undefined;
    readonly mcpManifests?: readonly RuntimeConfigPatchState[] | undefined;
    readonly pendingToolUses?: readonly RuntimeLoadedPendingToolUse[] | undefined;
    readonly backgroundTools?: readonly RuntimePreloadedBackgroundToolState[] | undefined;
    readonly pendingAttachments?: readonly ProviderRequestAttachment[] | undefined;
    readonly pendingAgentMail?: readonly RuntimeLoadedAgentMail[] | undefined;
    readonly coldCoverage: RuntimeColdCoverage;
  }> {
    const metadata = await this.metadata();
    const response = await loadContext(this.client, {
      scope: bridgeScope(input),
      runtimeInputId: input.runtimeInputId,
      sequenceFrom: input.sequenceFrom,
      sequenceTo: input.sequenceTo,
    }, metadata);
    if (!bridgeAckAccepted(response.ack?.status)) {
      throw normalizeContextLoaderError({
        code: "unavailable",
        sessionId: input.sessionId,
        reason: response.ack?.errorCode || "load context rejected",
      });
    }
    const parsed = parseContextPayload(response.contextJson, input);
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
}

/**
 * Persists Runtime Core semantic events, request ends, idle transitions, and runtime termination
 * through Bridge. Each write carries its thread scope and requires a committed or duplicate ACK;
 * rejected ACKs preserve closeout release sentinels, while unknown rejections remain retryable.
 */
export class BridgeAPIEventWriter implements SessionEventWriter {
  private readonly client: AgentRuntimeBridgeServiceClient;
  private readonly metadataFactory: (config: ServiceAccountTokenConfig) => Promise<Metadata>;

  constructor(private readonly options: BridgeAPIEventWriterOptions) {
    this.client = options.client ?? new AgentRuntimeBridgeServiceClient(options.address, credentials.createInsecure(), grpcClientChannelOptions());
    this.metadataFactory = options.metadataFactory ?? buildOutboundBearerMetadata;
  }

  /** Writes one semantic event and its optional projection, reasoning, or web-usage attachment. */
  async append(envelope: SessionEventEnvelope): Promise<SessionEventWriterAppendResult> {
    try {
      const metadata = await this.metadataFactory({ tokenPath: this.options.tokenPath });
      const event = sessionEventForDurableWrite(envelope.event);
      const request = {
        scope: bridgeScope(envelope),
        runtimeWriteId: envelope.writeId,
        modelRequestId: envelope.modelRequestId ?? modelRequestIdForEvent(event),
        eventType: event.type,
        payloadJson: JSON.stringify(event),
        sessionVisible: false,
        stableReasoningParts: (envelope.stableReasoningParts ?? []).map((part) => ({
          reasoningPartId: part.reasoningPartId,
          providerPartId: part.providerPartId ?? "",
          partSequence: part.partSequence,
          text: part.text,
          metadataJson: JSON.stringify(part.providerMetadata ?? {}),
          truncated: part.truncated,
        })),
        serverToolUse: envelope.serverToolUse,
        mcpMaterializationHandle: envelope.mcpMaterializationHandle,
        drafts: envelope.drafts.map(runtimeMessageDraftForBridge),
      };
      const declarationDigest = writeEventDeclarationDigest(request);
      const response = await writeEvent(this.client, request, metadata);
      if (!bridgeAckAccepted(response.ack?.status)) {
        return eventWriterRejected(envelope.sessionId, envelope.writeId, response.ack?.errorCode);
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
          sourceId: envelope.writeId,
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
        receipt.compactedThroughMessageSequence !== undefined
      ) {
        recordBridgeReceiptEvidence(this.options, {
          workspaceId: envelope.workspaceId,
          sessionId: envelope.sessionId,
          sessionThreadId: envelope.sessionThreadId,
          operation: "write_event",
          sourceKind: event.type,
          sourceId: envelope.writeId,
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
          sourceId: envelope.writeId,
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
        sourceId: envelope.writeId,
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
      return eventWriterTransportFailure(envelope.sessionId, envelope.writeId, error);
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
        stableReasoningParts: (envelope.stableReasoningParts ?? []).map((part) => ({
          reasoningPartId: part.reasoningPartId,
          providerPartId: part.providerPartId ?? "",
          partSequence: part.partSequence,
          text: part.text,
          metadataJson: JSON.stringify(part.providerMetadata ?? {}),
          truncated: part.truncated,
        })),
        usageJson: JSON.stringify({
          input_tokens: inputTokens,
          cache_read_input_tokens: cacheReadTokens,
          cache_creation_input_tokens: cacheWriteTokens,
          output_tokens: outputTokens,
          reasoning_output_tokens: envelope.usage?.reasoningTokens ?? 0,
          total_tokens: envelope.usage?.totalTokens ?? inputTokens + outputTokens,
          provider_usage_json: envelope.usage?.providerUsageJson ?? "{}",
        }),
        drafts: (envelope.drafts ?? []).map(runtimeMessageDraftForBridge),
        prefixConsumption: envelope.prefixConsumption === undefined
          ? undefined
          : {
              childThreadId: envelope.prefixConsumption.childThreadId,
              parentBoundaryEventId: envelope.prefixConsumption.parentBoundaryEventId,
              checkpointRuntimeLocalId: envelope.prefixConsumption.checkpointRuntimeLocalId,
            },
        compactedThroughMessageSequence: envelope.compactedThroughMessageSequence,
        compactionEventPayloadJson: envelope.compactionEventPayloadJson ?? "",
        interruptSettlement: envelope.interruptSettlement === undefined
          ? undefined
          : {
              runtimeInputId: envelope.interruptSettlement.runtimeInputId,
              eventIds: [...envelope.interruptSettlement.eventIds],
              sequenceFrom: envelope.interruptSettlement.sequenceFrom,
              sequenceTo: envelope.interruptSettlement.sequenceTo,
              pendingToolCancellations: envelope.interruptSettlement.pendingToolCancellations.map((pending) => ({
                toolUseEventId: pending.toolUseEventId,
                runtimeLocalId: pending.runtimeLocalId,
              })),
            },
      };
      const declarationDigest = writeRequestEndDeclarationDigest(request);
      const interruptRequest = request.interruptSettlement === undefined
        ? undefined
        : {
            scope: request.scope,
            runtimeInputId: request.interruptSettlement.runtimeInputId,
            eventIds: request.interruptSettlement.eventIds,
            sequenceFrom: request.interruptSettlement.sequenceFrom,
            sequenceTo: request.interruptSettlement.sequenceTo,
            inputKind: "interrupt_control",
            drafts: request.drafts.filter(
              (draft) => draft.draftKind === RuntimeDraftKind.RUNTIME_DRAFT_KIND_CANCELLATION,
            ),
            pendingToolCancellations: request.interruptSettlement.pendingToolCancellations,
          };
      const interruptDigest = interruptRequest === undefined
        ? undefined
        : commitInputsDeclarationDigest(interruptRequest);
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
          sourceId: envelope.modelRequestId,
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
        candidate.sourceId === envelope.modelRequestId
      );
      const interruptReceipt = interruptRequest === undefined
        ? undefined
        : receipts.find((candidate) =>
            candidate.operationKind === "commit_inputs" &&
            candidate.sourceKind === "interrupt_control" &&
            candidate.sourceId === interruptRequest.runtimeInputId
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
          sourceId: envelope.modelRequestId,
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
          sourceId: envelope.modelRequestId,
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
        sourceId: envelope.modelRequestId,
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
        scope: bridgeScope({ ...envelope, requestId: envelope.durableTurnId }),
        durableTurnId: envelope.durableTurnId,
        stopReasonJson: JSON.stringify(envelope.stopReason),
        drafts: (envelope.drafts ?? []).map(runtimeMessageDraftForBridge),
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
          sourceId: envelope.durableTurnId,
          declarationDigest,
          bindingId: envelope.bindingId,
          bindingGeneration: envelope.bindingGeneration,
          outcome: "receipt_shape_invalid",
        });
        return eventWriterSchemaFailure(envelope.sessionId, envelope.durableTurnId);
      }
      const receipt = runtimeDeclarationReceipt(bridgeReceipt);
      const applicationDisposition = runtimeReceiptApplicationDisposition(declaration.applicationDisposition);
      const completionDraft = envelope.drafts?.[0];
      const completionMessage = receipt.messages[0];
      const completionEvent = receipt.events[1];
      const completionReceiptMatches = completionDraft === undefined
        ? receipt.messages.length === 0 && receipt.events.length === 1
        : (
          receipt.messages.length === 1 &&
          receipt.events.length === 2 &&
          completionMessage !== undefined &&
          completionEvent !== undefined &&
          completionMessage.runtimeLocalId === completionDraft.runtimeLocalId &&
          completionMessage.sessionThreadId === envelope.sessionThreadId &&
          completionMessage.owningEventId === completionEvent.eventId &&
          completionMessage.parts.length === completionDraft.parts.length &&
          completionMessage.parts.every((part, index) =>
            part.runtimeLocalPartId === completionDraft.parts[index]?.runtimeLocalPartId &&
            part.messageId === completionMessage.messageId
          )
        );
      if (
        receipt.declarationDigest !== declarationDigest ||
        !completionReceiptMatches ||
        receipt.idleCloseout === undefined ||
        receipt.idleCloseout.durableTurnId !== envelope.durableTurnId ||
        receipt.idleCloseout.idleEventId !== receipt.events[0]?.eventId ||
        receipt.idleCloseout.idleEventSequence !== receipt.events[0]?.eventSequence ||
        receipt.pendingAttachmentDelta.length !== 0 ||
        receipt.pendingToolDelta.length !== 0 ||
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
          sourceId: envelope.durableTurnId,
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
        sourceId: envelope.durableTurnId,
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
  async commitRuntimeTermination(envelope: SessionEventWriterRuntimeTerminationEnvelope): Promise<SessionEventWriterAppendResult> {
    try {
      const metadata = await this.metadataFactory({ tokenPath: this.options.tokenPath });
      const request: CommitRuntimeTerminationRequest = {
        scope: bridgeScope(envelope),
        runtimeWriteId: envelope.writeId,
        failureJson: JSON.stringify(envelope.failure),
        drafts: envelope.drafts.map(runtimeMessageDraftForBridge),
        pendingToolCancellations: envelope.pendingToolCancellations.map((pending) => ({
          toolUseEventId: pending.toolUseEventId,
          runtimeLocalId: pending.runtimeLocalId,
        })),
      };
      const declarationDigest = runtimeTerminationDeclarationDigest(request);
      const response = await commitRuntimeTermination(this.client, request, metadata);
      if (!bridgeAckAccepted(response.ack?.status)) {
        return eventWriterRejected(envelope.sessionId, envelope.writeId, response.ack?.errorCode);
      }
      const declaration = response.declaration;
      const bridgeReceipt = declaration?.receipts[0];
      const applicationDisposition = declaration === undefined
        ? undefined
        : runtimeReceiptApplicationDisposition(declaration.applicationDisposition);
      if (
        declaration === undefined ||
        declaration.receipts.length !== 1 ||
        bridgeReceipt === undefined ||
        applicationDisposition === undefined ||
        bridgeReceipt.declarationDigest !== declarationDigest ||
        bridgeReceipt.compactedThroughMessageSequence !== undefined ||
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
          operation: "commit_runtime_termination",
          sourceKind: "runtime_termination",
          sourceId: envelope.writeId,
          declarationDigest,
          bindingId: declaration?.observedBindingId ?? envelope.bindingId,
          bindingGeneration: declaration?.observedBindingGeneration ?? envelope.bindingGeneration,
          ...(applicationDisposition === undefined ? {} : { applicationDisposition }),
          outcome: bridgeReceipt !== undefined && bridgeReceipt.declarationDigest !== declarationDigest
            ? "declaration_digest_mismatch"
            : applicationDisposition === "current_custody" &&
                (
                  declaration?.observedBindingId !== envelope.bindingId ||
                  declaration.observedBindingGeneration !== envelope.bindingGeneration
                )
              ? "binding_identity_mismatch"
              : "receipt_shape_invalid",
        });
        return eventWriterSchemaFailure(envelope.sessionId, envelope.writeId);
      }
      const receipt = runtimeDeclarationReceipt(bridgeReceipt);
      recordBridgeReceiptEvidence(this.options, {
        workspaceId: envelope.workspaceId,
        sessionId: envelope.sessionId,
        sessionThreadId: envelope.sessionThreadId,
        operation: "commit_runtime_termination",
        sourceKind: "runtime_termination",
        sourceId: envelope.writeId,
        declarationDigest,
        bindingId: declaration.observedBindingId,
        bindingGeneration: declaration.observedBindingGeneration,
        applicationDisposition,
        outcome: applicationDisposition === "current_custody" ? "applied" : "stale_custody",
      });
      return {
        ok: true,
        writeId: response.ack?.runtimeWriteId ?? "",
        eventId: receipt.events[0]?.eventId ?? response.ack?.runtimeWriteId ?? envelope.writeId,
        processedAt: new Date().toISOString(),
        declaration: {
          receipt,
          applicationDisposition,
          observedBindingId: declaration.observedBindingId,
          observedBindingGeneration: declaration.observedBindingGeneration,
        },
      };
    } catch (error) {
      return eventWriterTransportFailure(envelope.sessionId, envelope.writeId, error);
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

  /** Commits one unstamped repair draft and returns its database receipt. */
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
      drafts: [runtimeMessageDraftForBridge(repair.draft)],
    };
    const declarationDigest = internalToolRepairDeclarationDigest(request);
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
        sourceId: repair.repairKey,
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
        sourceId: repair.repairKey,
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
      sourceId: repair.repairKey,
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

function createChildThread(
  client: AgentRuntimeBridgeServiceClient,
  request: CreateChildThreadRequest,
  metadata: Metadata,
): Promise<CreateChildThreadResponse> {
  return new Promise((resolve, reject) => {
    client.createChildThread(request, metadata, (error: ServiceError | null, response: CreateChildThreadResponse) => {
      if (error !== null) {
        reject(error);
        return;
      }
      resolve(response);
    });
  });
}

function markChildThreadClosed(
  client: AgentRuntimeBridgeServiceClient,
  request: MarkChildThreadClosedRequest,
  metadata: Metadata,
): Promise<MarkChildThreadClosedResponse> {
  return new Promise((resolve, reject) => {
    client.markChildThreadClosed(request, metadata, (error: ServiceError | null, response: MarkChildThreadClosedResponse) => {
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

function resolveInterAgentDelivery(
  client: AgentRuntimeBridgeServiceClient,
  request: ResolveInterAgentDeliveryRequest,
  metadata: Metadata,
): Promise<ResolveInterAgentDeliveryResponse> {
  return new Promise((resolve, reject) => {
    client.resolveInterAgentDelivery(request, metadata, (error: ServiceError | null, response: ResolveInterAgentDeliveryResponse) => {
      if (error !== null) {
        reject(error);
        return;
      }
      resolve(response);
    });
  });
}

function runtimeMessageDraftForBridge(draft: CoreRuntimeMessageDraft) {
  const {
    runtimeLocalId,
    sourceKind,
    sourceId,
    sourceEventId,
    draftKind,
    ordinal,
    parts,
    ...messageInfo
  } = draft;
  return {
    runtimeLocalId,
    sourceKind,
    sourceId,
    sourceEventId: sourceEventId ?? "",
    draftKind: runtimeDraftKindForBridge(draftKind),
    ordinal,
    messageInfoJson: JSON.stringify(messageInfo),
    parts: parts.map((part) => {
      const {
        runtimeLocalPartId,
        ordinal: partOrdinal,
        ...partInfo
      } = part;
      return {
        runtimeLocalPartId,
        partKind: part.type,
        ordinal: partOrdinal,
        partJson: JSON.stringify(partInfo),
      };
    }),
  };
}

function runtimeDraftKindForBridge(kind: CoreRuntimeMessageDraft["draftKind"]): RuntimeDraftKind {
  switch (kind) {
    case "user_input":
      return RuntimeDraftKind.RUNTIME_DRAFT_KIND_USER_INPUT;
    case "approval_input":
      return RuntimeDraftKind.RUNTIME_DRAFT_KIND_APPROVAL_INPUT;
    case "reviewer_input":
      return RuntimeDraftKind.RUNTIME_DRAFT_KIND_REVIEWER_INPUT;
    case "agent_mail_input":
      return RuntimeDraftKind.RUNTIME_DRAFT_KIND_AGENT_MAIL_INPUT;
    case "assistant_text":
      return RuntimeDraftKind.RUNTIME_DRAFT_KIND_ASSISTANT_TEXT;
    case "tool_use":
      return RuntimeDraftKind.RUNTIME_DRAFT_KIND_TOOL_USE;
    case "tool_result":
      return RuntimeDraftKind.RUNTIME_DRAFT_KIND_TOOL_RESULT;
    case "task_notification":
      return RuntimeDraftKind.RUNTIME_DRAFT_KIND_TASK_NOTIFICATION;
    case "rejection":
      return RuntimeDraftKind.RUNTIME_DRAFT_KIND_REJECTION;
    case "cancellation":
      return RuntimeDraftKind.RUNTIME_DRAFT_KIND_CANCELLATION;
    case "completion_mail":
      return RuntimeDraftKind.RUNTIME_DRAFT_KIND_COMPLETION_MAIL;
    case "compaction_checkpoint":
      return RuntimeDraftKind.RUNTIME_DRAFT_KIND_COMPACTION_CHECKPOINT;
    case "internal_tool_repair":
      return RuntimeDraftKind.RUNTIME_DRAFT_KIND_INTERNAL_TOOL_REPAIR;
    case "termination":
      return RuntimeDraftKind.RUNTIME_DRAFT_KIND_TERMINATION;
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
  return {
    sessionThreadId: receipt.sessionThreadId,
    operationKind: receipt.operationKind,
    sourceKind: receipt.sourceKind,
    sourceId: receipt.sourceId,
    declarationDigest: receipt.declarationDigest,
    pendingAttachmentDelta: parsePendingAttachments(
      receipt.pendingAttachmentDeltaJson.map((item) => JSON.parse(item) as unknown),
    ) ?? [],
    pendingToolDelta: [...receipt.pendingToolDeltaJson],
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
    ...(receipt.sealedAgentMail === undefined ? {} : { sealedAgentMail: (() => {
      const stamp = receipt.sealedAgentMail;
      if (
        stamp.disposition !== SealedAgentMailDisposition.SEALED_AGENT_MAIL_DISPOSITION_SEALED_FOR_FAILED_BIRTH ||
        stamp.runtimeInputId.length === 0 ||
        stamp.birthPreparationAttemptId.length === 0 ||
        stamp.failedPreparationAttemptId.length === 0
      ) {
        throw new Error("declaration receipt has an invalid sealed-agent-mail stamp");
      }
      return {
        runtimeInputId: stamp.runtimeInputId,
        birthPreparationAttemptId: stamp.birthPreparationAttemptId,
        failedPreparationAttemptId: stamp.failedPreparationAttemptId,
        disposition: "sealed_for_failed_birth" as const,
      };
    })() }),
    events: receipt.events.map((event) => ({
      sessionThreadId: event.sessionThreadId,
      sourceEventId: event.sourceEventId,
      eventId: event.eventId,
      eventSequence: event.eventSequence,
      disposition: runtimeEventDisposition(event.disposition),
    })),
    messages: receipt.messages.map((message) => ({
      runtimeLocalId: message.runtimeLocalId,
      sessionThreadId: message.sessionThreadId,
      owningEventId: message.owningEventId,
      messageId: message.messageId,
      messageSequence: message.messageSequence,
      createdAt: message.createdAt,
      updatedAt: message.updatedAt,
      disposition: runtimeProjectionDisposition(message.disposition),
      parts: message.parts.map((part) => ({
        runtimeLocalPartId: part.runtimeLocalPartId,
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

export function validateChildLifecycleDeclarationResponse(
  input: {
    readonly action: "close" | "resume";
    readonly sessionThreadId: string;
    readonly childThreadId: string;
    readonly sourceKind: "tool_use" | "approval_review";
    readonly sourceCommandId: string;
    readonly requestedAt: string;
    readonly bindingId: string;
    readonly bindingGeneration: number;
  },
  response: MarkChildThreadClosedResponse | MarkChildThreadActiveResponse,
): { readonly ok: true; readonly dispositions: readonly RuntimeDeclarationReceipt["childLifecycle"][number][] } |
  { readonly ok: false; readonly errorCode: string; readonly discardHotState: boolean } {
  if (!bridgeAckAccepted(response.ack?.status)) {
    return {
      ok: false,
      errorCode: response.ack?.errorCode || "bridge_child_lifecycle_rejected",
      discardHotState: false,
    };
  }
  const operationKind = input.action === "close" ? "mark_child_thread_closed" : "mark_child_thread_active";
  const sourceKind = input.action === "close" ? "child_close_command" : "child_resume_command";
  const operationId = stableRuntimeID(
    input.action === "close" ? "child_tree_close" : "child_resume",
    input.sourceCommandId,
    input.childThreadId,
  );
  const declaration = response.declaration;
  const applicationDisposition = declaration === undefined
    ? undefined
    : runtimeReceiptApplicationDisposition(declaration.applicationDisposition);
  if (
    declaration === undefined ||
    applicationDisposition === undefined ||
    declaration.receipts.length === 0 ||
    declaration.observedBindingId !== input.bindingId ||
    declaration.observedBindingGeneration !== input.bindingGeneration
  ) {
    return {
      ok: false,
      errorCode: "bridge_child_lifecycle_receipt_invalid",
      discardHotState: true,
    };
  }
  if (applicationDisposition !== "current_custody") {
    return { ok: false, errorCode: "scope_superseded", discardHotState: true };
  }
  let receipts: RuntimeDeclarationReceipt[];
  try {
    receipts = declaration.receipts.map(ordinaryRuntimeDeclarationReceipt);
  } catch {
    return {
      ok: false,
      errorCode: "bridge_child_lifecycle_receipt_invalid",
      discardHotState: true,
    };
  }
  const digest = childLifecycleDeclarationDigest({
    operationKind,
    action: input.action,
    sessionThreadId: input.sessionThreadId,
    childThreadId: input.childThreadId,
    sourceKind: input.sourceKind,
    sourceCommandId: input.sourceCommandId,
    requestedAt: input.requestedAt,
  });
  const stamps = receipts.flatMap((receipt) => receipt.childLifecycle);
  const receiptTargets = new Set(receipts.map((receipt) => receipt.sessionThreadId));
  const validDisposition = input.action === "close"
    ? (value: string): boolean =>
        value === "closed" ||
        value === "already_closed" ||
        value === "preserved_failed" ||
        value === "preserved_terminated"
    : (value: string): boolean =>
        value === "resumed" ||
        value === "already_active" ||
        value === "preserved_failed" ||
        value === "preserved_terminated";
  if (
    receiptTargets.size !== receipts.length ||
    !receiptTargets.has(input.childThreadId) ||
    receipts.some((receipt) =>
      receipt.operationKind !== operationKind ||
      receipt.sourceKind !== sourceKind ||
      receipt.sourceId !== operationId ||
      receipt.declarationDigest !== digest ||
      receipt.events.length !== 0 ||
      receipt.messages.length !== 0 ||
      receipt.pendingAttachmentDelta.length !== 0 ||
      receipt.pendingToolDelta.length !== 0 ||
      receipt.prefixConsumptions.length !== 0 ||
      receipt.requestReschedule !== undefined ||
      receipt.idleCloseout !== undefined ||
      receipt.compactedThroughMessageSequence !== undefined ||
      receipt.sealedAgentMail !== undefined ||
      receipt.childLifecycle.length !== 1 ||
      receipt.childLifecycle[0]?.childThreadId !== receipt.sessionThreadId ||
      receipt.childLifecycle[0]?.effectiveAt !== input.requestedAt ||
      !validDisposition(receipt.childLifecycle[0]?.disposition ?? "")
    ) ||
    (input.action === "resume" && (receipts.length !== 1 || receipts[0]?.sessionThreadId !== input.childThreadId))
  ) {
    return {
      ok: false,
      errorCode: "bridge_child_lifecycle_receipt_invalid",
      discardHotState: true,
    };
  }
  return { ok: true, dispositions: stamps };
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
  readonly requestId: string;
  readonly workspaceId: string;
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly bindingId: string;
  readonly bindingGeneration: number;
  readonly targetPodUid: string;
}): RuntimeScope {
  return {
    requestId: input.requestId,
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

function bindingTokenRefreshScope(identity: RuntimeSessionIdentity): RuntimeScope {
  return {
    requestId: `binding-token-refresh:${identity.sessionThreadId}`,
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

function bindingTokenRefreshKey(identity: RuntimeSessionIdentity): string {
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
  const code = typeof error === "object" && error !== null && "code" in error ? (error as { readonly code?: unknown }).code : undefined;
  return code === status.FAILED_PRECONDITION || code === status.UNAVAILABLE || code === status.DEADLINE_EXCEEDED;
}

function approvalReviewerParentScope(input: ApprovalReviewerThreadCreation): RuntimeScope {
  return {
    requestId: input.request.modelRequestId,
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

function approvalReviewerCreateChildThreadRequest(input: ApprovalReviewerThreadCreation): CreateChildThreadRequest {
  return {
    scope: approvalReviewerParentScope(input),
    parentThreadId: input.request.sessionThreadId,
    childThreadId: input.reviewerThreadId,
    role: "approval_reviewer",
    taskName: "",
    metadataJson: "{}",
    agentType: "approval_reviewer",
    sourceToolUseEventId: "",
    forkTurns: input.isTrunk ? "none" : "all",
    threadContextPrefixJson: input.threadContextPrefixJson,
    isTrunk: input.isTrunk,
    reviewerReviewId: input.isTrunk ? "" : input.reviewId,
  };
}

function bridgeAckAccepted(status: BridgeWriteStatus | undefined): boolean {
  return status === BridgeWriteStatus.BRIDGE_WRITE_STATUS_COMMITTED || status === BridgeWriteStatus.BRIDGE_WRITE_STATUS_DUPLICATE;
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

function parseContextPayload(contextJson: string, input: RuntimeThreadControlState): {
  readonly messages: readonly RuntimeMessage[];
  readonly threadContextPrefix?: ThreadContextPrefix | undefined;
  readonly durableTurnId?: string | undefined;
  readonly thread: RuntimeAcceptedThreadMetadataState;
  readonly runtimeConfigPatch?: RuntimeConfigPatchState | undefined;
  readonly mcpManifests?: readonly RuntimeConfigPatchState[] | undefined;
  readonly pendingToolUses?: readonly RuntimeLoadedPendingToolUse[] | undefined;
  readonly backgroundTools?: readonly RuntimePreloadedBackgroundToolState[] | undefined;
  readonly pendingAttachments?: readonly ProviderRequestAttachment[] | undefined;
  readonly pendingAgentMail?: readonly RuntimeLoadedAgentMail[] | undefined;
  readonly coldCoverage: RuntimeColdCoverage;
} {
  try {
    const parsed = JSON.parse(contextJson) as Record<string, unknown>;
    if (!Array.isArray(parsed.messages)) {
      throw new Error("load context messages are malformed");
    }
    const messages = parsed.messages.map((message) => DurableRuntimeMessageSchema.parse(message));
    const threadContextPrefix = parseThreadContextPrefix(parsed.threadContextPrefix);
    const durableTurnId = stringField(parsed, "durableTurnId");
    const thread = parseThreadMetadata(parsed.thread);
    const runtimeConfigPatch = runtimeConfigPatchFromContextPayload(parsed, input);
    const mcpManifests = mcpManifestPatchesFromContextPayload(parsed.mcpManifests, input);
    const pendingToolUses = parsePendingToolUses(parsed.pendingToolUses);
    const backgroundTools = parseBackgroundTools(parsed.backgroundTools);
    const pendingAttachments = parsePendingAttachments(parsed.pendingAttachments);
    const pendingAgentMail = parsePendingAgentMail(parsed.pendingAgentMail);
    const coldCoverage = parseColdCoverage(parsed.coldCoverage);
    return {
      messages,
      ...(threadContextPrefix !== undefined ? { threadContextPrefix } : {}),
      ...(durableTurnId !== undefined ? { durableTurnId } : {}),
      thread,
      ...(runtimeConfigPatch !== undefined ? { runtimeConfigPatch } : {}),
      ...(mcpManifests !== undefined ? { mcpManifests } : {}),
      ...(pendingToolUses !== undefined ? { pendingToolUses } : {}),
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
    pendingAttachmentIdentities: stringList("pendingAttachmentIdentities"),
    undeliveredMailDeliveryIds: stringList("undeliveredMailDeliveryIds"),
  };
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

function parsePendingAgentMail(value: unknown): readonly RuntimeLoadedAgentMail[] | undefined {
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
    return {
      deliveryId: requiredStringField(item, "deliveryId"),
      sourceThreadId: requiredStringField(item, "sourceThreadId"),
      sourceToolUseEventId: requiredStringField(item, "sourceToolUseEventId"),
    };
  });
  if (parsed.length > MailFetchMaxEnvelopes) {
    throw new Error("load context pendingAgentMail exceeds the envelope bound");
  }
  return parsed;
}

function parsePendingAttachments(value: unknown): readonly ProviderRequestAttachment[] | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (!Array.isArray(value)) {
    throw new Error("load context pendingAttachments is malformed");
  }
  return value.map((item): ProviderRequestAttachment => {
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
  input: RuntimeThreadControlState,
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
      runtimeInputId: `runtime_config_update:mcp_manifest:${input.sessionId}:${mcpServerName}:${generation}`,
      generation,
      mcpServerName,
      ...(manifestETag !== undefined ? { manifestETag } : {}),
      manifestReadiness,
      ...(diagnostic !== undefined ? { manifestDiagnostic: diagnostic } : {}),
      payloadJson: JSON.stringify({
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
  input: RuntimeThreadControlState,
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
    generation,
    coldLoad: true,
    installedBuiltinFamily,
    payloadJson: JSON.stringify({
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
    const kind = stringField(item, "kind");
    const status = stringField(item, "status");
    if ((kind !== "approval" && kind !== "custom") || (status !== "pending" && status !== "resolving")) {
      throw new Error("load context pendingToolUses is malformed");
    }
    const decision = pendingToolDecision(item);
    const denyMessage = stringField(item, "denyMessage");
    return {
      toolUseEventId: requiredStringField(item, "toolUseEventId"),
      modelRequestId: requiredStringField(item, "modelRequestId"),
      modelToolCallId: requiredStringField(item, "modelToolCallId"),
      toolName: requiredStringField(item, "toolName"),
      kind,
      input: RuntimeJsonValueSchema.parse(item.input ?? {}),
      ...(decision !== undefined ? { decision } : {}),
      ...(denyMessage !== undefined ? { denyMessage } : {}),
      status,
      expiresAt: requiredStringField(item, "expiresAt"),
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
): SessionEventWriterAppendResult {
  const code =
    errorCode === "scope_superseded"
      ? "superseded"
      : errorCode === "closeout_unrepairable"
        ? "unrepairable"
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

function eventWriterTransportFailure(
  sessionId: string,
  writeId: string,
  error: unknown,
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
        : "unknown";
  return {
    ok: false,
    error: normalizeSessionEventWriterError({ code, rawError: error, sessionId, writeId }),
  };
}
