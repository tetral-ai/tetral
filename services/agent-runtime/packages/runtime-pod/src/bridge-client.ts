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
import type { Metadata, ServiceError } from "@grpc/grpc-js";
import {
  AgentRuntimeBridgeServiceClient,
  BridgeWriteStatus,
  DurableEventDisposition,
  DurableProjectionDisposition,
  ReceiptApplicationDisposition,
  RuntimeDraftKind,
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
  MarkChildThreadClosedRequest,
  MarkChildThreadClosedResponse,
  RefreshRuntimeBindingTokenRequest,
  RefreshRuntimeBindingTokenResponse,
  RuntimeScope,
  WriteEventRequest,
  WriteEventResponse,
  WriteRequestEndRequest,
  WriteRequestEndResponse,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type { ProviderRequestAttachment } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { AcceptedInputCommitResult, ContextLoader, RuntimeLoadedAgentMail, RuntimeLoadedPendingToolUse } from "@tetral/agent-runtime-core/src/context/context-loader.js";
import type {
  RuntimeAcceptedInputState,
  RuntimeAcceptedThreadMetadataState,
  RuntimeConfigPatchState,
  RuntimePreloadedBackgroundToolState,
  RuntimeThreadControlState,
} from "@tetral/agent-runtime-core/src/session/session-state.js";
import type { RuntimeSessionIdentity } from "@tetral/agent-runtime-core/src/session/session.js";
import type { ThreadContextPrefix } from "@tetral/agent-runtime-core/src/session/context-manager.js";
import {
  DurableRuntimeMessageSchema,
  RuntimeMessageSchema,
  RuntimeJsonValueSchema,
  normalizeContextLoaderError,
  normalizeRuntimeMessageStoreError,
  normalizeSessionEventWriterError,
} from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import {
  MailFetchMaxBodyBytes,
  MailFetchMaxEnvelopes,
} from "@tetral/agent-runtime-protocol/src/bounds.js";
import type {
  RuntimeMessageDraft as CoreRuntimeMessageDraft,
  RuntimeInternalToolRepairCommit,
  RuntimeMessage,
  RuntimeMessageStoreWritePartResult,
  SessionEventEnvelope,
  SessionEventWriter,
  SessionEventWriterAppendResult,
  SessionEventWriterFinishIdleEnvelope,
  SessionEventWriterRequestEndEnvelope,
  SessionEventWriterRuntimeTerminationEnvelope,
} from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import type { RuntimeDeclarationReceipt } from "@tetral/agent-runtime-core/src/runtime/runtime-declaration.js";
import { buildOutboundBearerMetadata } from "./auth.js";
import type { ServiceAccountTokenConfig } from "./auth.js";
import type { ApprovalReviewerThreadCreation, RuntimeApprovalReviewerThreadCreator } from "./approval-reviewer.js";
import type { RuntimeControlInputCommitter, RuntimeTaskNotificationCommitter } from "./runtime-service.js";
import { bridgeAttachmentGrpcChannelOptions, grpcClientChannelOptions } from "./bounds.js";
import { sessionEventForDurableWrite } from "@tetral/agent-runtime-core/src/runtime/session-event-writer.js";
import { commitInputsDeclarationDigest } from "./runtime-declaration-wire.js";

/** Configures the Bridge adapter that durably settles interrupt and tool-confirmation inputs. */
export interface BridgeAPIControlInputCommitterOptions {
  readonly address: string;
  readonly tokenPath: string;
  readonly metadataFactory?: (config: ServiceAccountTokenConfig) => Promise<Metadata>;
  readonly client?: AgentRuntimeBridgeServiceClient;
}

/**
 * Commits control inputs through Bridge before Runtime Core treats the control action as durable.
 * Authentication and transport failures remain retryable unless the gRPC status identifies a
 * deterministic request or idempotency conflict; only committed or duplicate ACKs succeed.
 */
export class BridgeAPIControlInputCommitter implements RuntimeControlInputCommitter {
  private readonly client: AgentRuntimeBridgeServiceClient;
  private readonly metadataFactory: (config: ServiceAccountTokenConfig) => Promise<Metadata>;

  constructor(private readonly options: BridgeAPIControlInputCommitterOptions) {
    this.client = options.client ?? new AgentRuntimeBridgeServiceClient(options.address, credentials.createInsecure(), grpcClientChannelOptions());
    this.metadataFactory = options.metadataFactory ?? buildOutboundBearerMetadata;
  }

  /** Commits one interrupt or tool-confirmation input without projecting a message patch. */
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
    let response: CommitInputsResponse;
    try {
      response = await commitInputs(this.client, {
        scope: bridgeScope(input.scope),
        runtimeInputId: input.scope.runtimeInputId,
        eventIds: [...input.scope.eventIds],
        sequenceFrom: input.scope.sequenceFrom,
        sequenceTo: input.scope.sequenceTo,
        hotContextPatchJson: "{}",
        inputKind: input.inputKind,
        interAgentMessageJson: "",
        approvalReviewJson: "",
        drafts: [],
      }, metadata);
    } catch (error) {
      return {
        ok: false as const,
        retryable: bridgeCommitErrorRetryable(error),
        errorCode: "bridge_commit_unavailable",
        message: "control input durable commit failed",
      };
    }
    if (bridgeAckAccepted(response.ack?.status)) {
      return { ok: true as const };
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
export interface BridgeAPITaskNotificationCommitterOptions {
  readonly address: string;
  readonly tokenPath: string;
  readonly metadataFactory?: (config: ServiceAccountTokenConfig) => Promise<Metadata>;
  readonly client?: AgentRuntimeBridgeServiceClient;
}

/**
 * Settles a terminal background-task notification through Bridge and validates the returned
 * runtime-only user projection before handing it to Runtime Core. A stale Bridge disposition is
 * an idempotent success, while malformed committed projections fail without retry.
 */
export class BridgeAPITaskNotificationCommitter implements RuntimeTaskNotificationCommitter {
  private readonly client: AgentRuntimeBridgeServiceClient;
  private readonly metadataFactory: (config: ServiceAccountTokenConfig) => Promise<Metadata>;

  constructor(private readonly options: BridgeAPITaskNotificationCommitterOptions) {
    this.client = options.client ?? new AgentRuntimeBridgeServiceClient(options.address, credentials.createInsecure(), grpcClientChannelOptions());
    this.metadataFactory = options.metadataFactory ?? buildOutboundBearerMetadata;
  }

  /** Commits one task notification and returns its validated RuntimeMessage or stale disposition. */
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
    let response: CommitTaskNotificationResultResponse;
    try {
      response = await commitTaskNotificationResult(this.client, {
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
      }, metadata);
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
      const runtimeMessage = parseTaskNotificationRuntimeMessage(response.runtimeMessageJson, input.scope.sessionId);
      if (runtimeMessage === undefined) {
        return {
          ok: false as const,
          retryable: false,
          errorCode: "bridge_task_notification_projection_invalid",
          message: "task notification durable commit returned malformed projection",
        };
      }
      return { ok: true as const, runtimeMessage };
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
export interface BridgeAPIApprovalReviewerThreadCreatorOptions {
  readonly address: string;
  readonly tokenPath: string;
  readonly metadataFactory?: (config: ServiceAccountTokenConfig) => Promise<Metadata>;
  readonly client?: AgentRuntimeBridgeServiceClient;
  readonly releaseThreadScope?: (workspaceId: string, sessionId: string, sessionThreadId: string) => void;
}

/**
 * Manages approval-reviewer child-thread rows through Bridge. The approval reviewer calls this
 * adapter around its execution, and a successful close ACK releases the matching local thread
 * scope through the optional callback.
 */
export class BridgeAPIApprovalReviewerThreadCreator implements RuntimeApprovalReviewerThreadCreator {
  private readonly client: AgentRuntimeBridgeServiceClient;
  private readonly metadataFactory: (config: ServiceAccountTokenConfig) => Promise<Metadata>;

  constructor(private readonly options: BridgeAPIApprovalReviewerThreadCreatorOptions) {
    this.client = options.client ?? new AgentRuntimeBridgeServiceClient(options.address, credentials.createInsecure(), grpcClientChannelOptions());
    this.metadataFactory = options.metadataFactory ?? buildOutboundBearerMetadata;
  }

  /** Creates a seedless trunk or a sidecar whose fork-seed snapshot may be empty, ACK-gated. */
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
    try {
      response = await markChildThreadClosed(this.client, {
        scope: approvalReviewerParentScope(input),
        childThreadId: input.reviewerThreadId,
        closedAt: new Date().toISOString(),
      }, metadata);
    } catch {
      return { ok: false as const, message: "approval reviewer thread close is unavailable" };
    }
    if (bridgeAckAccepted(response.ack?.status)) {
      this.options.releaseThreadScope?.(input.request.workspaceId, input.request.sessionId, input.reviewerThreadId);
      return { ok: true as const };
    }
    return {
      ok: false as const,
      message: response.ack?.errorCode || "approval reviewer thread close was not acknowledged",
    };
  }
}

function parseTaskNotificationRuntimeMessage(runtimeMessageJson: string | undefined, sessionId: string): RuntimeMessage | undefined {
  if (runtimeMessageJson === undefined || runtimeMessageJson === "") {
    return undefined;
  }
  try {
    const message = RuntimeMessageSchema.parse(JSON.parse(runtimeMessageJson));
    if (message.sessionId !== sessionId || message.role !== "user" || message.origin !== "runtime") {
      return undefined;
    }
    return message;
  } catch {
    return undefined;
  }
}

/**
 * Configures Bridge context loading, binding-token refresh, and active per-thread scope tracking.
 */
export interface BridgeAPIContextLoaderOptions {
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
 * Implements Runtime Core's context loader with authenticated Bridge reads and writes. It commits
 * accepted input before loading the resulting context and validates message and recovery fields
 * before exposing any cold state to Runtime Core.
 * It tracks active scopes by session and thread and coalesces concurrent binding-token refreshes for
 * the same binding identity.
 */
export class BridgeAPIContextLoader implements ContextLoader {
  private readonly client: AgentRuntimeBridgeServiceClient;
  private readonly metadataFactory: (config: ServiceAccountTokenConfig) => Promise<Metadata>;
  private readonly activeInputsByThread = new Map<string, RuntimeAcceptedInputState>();
  private readonly scopedInputsByThread = new Map<string, Array<{ readonly token: symbol; readonly input: RuntimeAcceptedInputState }>>();
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

  /** Returns the latest temporary scope for a thread, falling back to its active input scope. */
  acceptedInputForThread(workspaceId: string, sessionId: string, sessionThreadId: string): RuntimeAcceptedInputState | undefined {
    const key = threadScopeKey(workspaceId, sessionId, sessionThreadId);
    return this.scopedInputsByThread.get(key)?.at(-1)?.input ?? this.activeInputsByThread.get(key);
  }

  /** Registers an active thread scope and returns cleanup that cannot remove a newer input. */
  registerAcceptedInput(input: RuntimeAcceptedInputState): () => void {
    const key = threadScopeKey(input.workspaceId, input.sessionId, input.sessionThreadId);
    this.activeInputsByThread.set(key, input);
    return () => {
      const current = this.activeInputsByThread.get(key);
      if (current?.runtimeInputId === input.runtimeInputId) {
        this.activeInputsByThread.delete(key);
      }
    };
  }

  /** Pushes a temporary thread scope and returns token-specific cleanup for nested registrations. */
  registerScopedAcceptedInput(input: RuntimeAcceptedInputState): () => void {
    const key = threadScopeKey(input.workspaceId, input.sessionId, input.sessionThreadId);
    const token = Symbol("scoped-accepted-input");
    const registrations = this.scopedInputsByThread.get(key) ?? [];
    registrations.push({ token, input });
    this.scopedInputsByThread.set(key, registrations);
    return () => {
      const current = this.scopedInputsByThread.get(key);
      if (current === undefined) {
        return;
      }
      const index = current.findIndex((registration) => registration.token === token);
      if (index < 0) {
        return;
      }
      current.splice(index, 1);
      if (current.length === 0) {
        this.scopedInputsByThread.delete(key);
      }
    };
  }

  /** Removes active and temporary scope registrations for one thread. */
  releaseAcceptedInputForThread(workspaceId: string, sessionId: string, sessionThreadId: string): void {
    const key = threadScopeKey(workspaceId, sessionId, sessionThreadId);
    this.activeInputsByThread.delete(key);
    this.scopedInputsByThread.delete(key);
  }

  /** Loads and validates the complete cold-start projection for the supplied thread command. */
  async loadThreadContext(
    command: RuntimeThreadControlState,
    options?: { readonly agentMailSourceThreadId?: string | undefined },
  ): Promise<{
    readonly messages: readonly RuntimeMessage[];
    readonly threadContextPrefix?: ThreadContextPrefix | undefined;
    readonly runtimeBindingToken: string;
    readonly thread: RuntimeAcceptedThreadMetadataState;
    readonly runtimeConfigPatch?: RuntimeConfigPatchState | undefined;
    readonly mcpManifests?: readonly RuntimeConfigPatchState[] | undefined;
    readonly pendingToolUses?: readonly RuntimeLoadedPendingToolUse[] | undefined;
    readonly backgroundTools?: readonly RuntimePreloadedBackgroundToolState[] | undefined;
    readonly pendingAttachments?: readonly ProviderRequestAttachment[] | undefined;
    readonly pendingAgentMail?: readonly RuntimeLoadedAgentMail[] | undefined;
  }> {
    return await this.loadContext(command, options);
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
    const metadata = await this.metadata();
    const scope = bridgeScope(input);
    const request = {
      scope,
      runtimeInputId: input.runtimeInputId,
      eventIds: [...input.eventIds],
      sequenceFrom: input.sequenceFrom,
      sequenceTo: input.sequenceTo,
      hotContextPatchJson: input.kind === "messages" ? input.payloadJson : "{}",
      inputKind: input.kind,
      interAgentMessageJson: input.kind === "inter_agent_message" ? interAgentCommitPayloadJson(input) : "",
      approvalReviewJson: input.kind === "approval_review" ? approvalReviewCommitPayloadJson(input) : "",
      drafts: drafts.map(runtimeMessageDraftForBridge),
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
      throw normalizeContextLoaderError({
        code: "schema_mismatch",
        sessionId: input.sessionId,
        reason: "commit inputs did not return exactly one declaration receipt",
      });
    }
    if (receipt.declarationDigest !== declarationDigest) {
      throw normalizeContextLoaderError({
        code: "schema_mismatch",
        sessionId: input.sessionId,
        reason: "commit inputs returned a mismatched declaration digest",
      });
    }
    const applicationDisposition = declaration.applicationDisposition ===
      ReceiptApplicationDisposition.RECEIPT_APPLICATION_DISPOSITION_CURRENT_CUSTODY
      ? "current_custody"
      : declaration.applicationDisposition === ReceiptApplicationDisposition.RECEIPT_APPLICATION_DISPOSITION_STALE_CUSTODY
        ? "stale_custody"
        : undefined;
    if (applicationDisposition === undefined) {
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
      throw normalizeContextLoaderError({
        code: "schema_mismatch",
        sessionId: input.sessionId,
        reason: "commit inputs returned mismatched current custody identity",
      });
    }
    return {
      type: "receipt",
      inputDisposition,
      applicationDisposition,
      receipt: runtimeDeclarationReceipt(receipt),
    };
  }

  private async loadContext(
    input: RuntimeThreadControlState,
    options?: { readonly agentMailSourceThreadId?: string | undefined },
  ): Promise<{
    readonly messages: readonly RuntimeMessage[];
    readonly threadContextPrefix?: ThreadContextPrefix | undefined;
    readonly runtimeBindingToken: string;
    readonly thread: RuntimeAcceptedThreadMetadataState;
    readonly runtimeConfigPatch?: RuntimeConfigPatchState | undefined;
    readonly mcpManifests?: readonly RuntimeConfigPatchState[] | undefined;
    readonly pendingToolUses?: readonly RuntimeLoadedPendingToolUse[] | undefined;
    readonly backgroundTools?: readonly RuntimePreloadedBackgroundToolState[] | undefined;
    readonly pendingAttachments?: readonly ProviderRequestAttachment[] | undefined;
    readonly pendingAgentMail?: readonly RuntimeLoadedAgentMail[] | undefined;
  }> {
    const metadata = await this.metadata();
    const response = await loadContext(this.client, {
      scope: bridgeScope(input),
      runtimeInputId: input.runtimeInputId,
      sequenceFrom: input.sequenceFrom,
      sequenceTo: input.sequenceTo,
      agentMailSourceThreadId: options?.agentMailSourceThreadId ?? "",
    }, metadata);
    if (!bridgeAckAccepted(response.ack?.status)) {
      throw normalizeContextLoaderError({
        code: "unavailable",
        sessionId: input.sessionId,
        reason: response.ack?.errorCode || "load context rejected",
      });
    }
    const parsed = parseContextPayload(
      response.contextJson,
      input,
      options?.agentMailSourceThreadId !== undefined && options.agentMailSourceThreadId !== "",
    );
    return { ...parsed, runtimeBindingToken: response.runtimeBindingToken };
  }

  private async metadata(): Promise<Metadata> {
    return await this.metadataFactory({ tokenPath: this.options.tokenPath });
  }
}

/**
 * Configures the Bridge event writer and resolves each write to the active thread binding scope.
 */
export interface BridgeAPIEventWriterOptions {
  readonly address: string;
  readonly tokenPath: string;
  readonly scopeForThread: (workspaceId: string, sessionId: string, sessionThreadId: string) => RuntimeAcceptedInputState | undefined;
  readonly metadataFactory?: (config: ServiceAccountTokenConfig) => Promise<Metadata>;
  readonly client?: AgentRuntimeBridgeServiceClient;
}

/**
 * Persists Runtime Core semantic events, request ends, idle transitions, and runtime termination
 * through Bridge. Each write requires a registered thread scope and a committed or duplicate ACK;
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
    const input = this.options.scopeForThread(envelope.workspaceId, envelope.sessionId, envelope.sessionThreadId);
    if (input === undefined) {
      return eventWriterUnavailable(envelope.sessionId, envelope.writeId);
    }
    try {
      const metadata = await this.metadataFactory({ tokenPath: this.options.tokenPath });
      const event = sessionEventForDurableWrite(envelope.event);
      const response = await writeEvent(this.client, {
        scope: bridgeScope(input),
        runtimeWriteId: envelope.writeId,
        modelRequestId: envelope.modelRequestId ?? modelRequestIdForEvent(event),
        eventType: event.type,
        payloadJson: JSON.stringify(event),
        projectionJson: envelope.projectionJson ?? "{}",
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
      }, metadata);
      if (!bridgeAckAccepted(response.ack?.status)) {
        return eventWriterRejected(envelope.sessionId, envelope.writeId, response.ack?.errorCode);
      }
      return {
        ok: true,
        writeId: response.ack?.runtimeWriteId ?? "",
        eventId: response.eventId,
        processedAt: new Date().toISOString(),
      };
    } catch (error) {
      return eventWriterTransportFailure(envelope.sessionId, envelope.writeId, error);
    }
  }

  /**
   * Writes a terminal model-request record, normalized usage, consumed attachments, optional
   * reasoning, and optional reschedule request, then validates Bridge's reschedule disposition.
   */
  async writeRequestEnd(envelope: SessionEventWriterRequestEndEnvelope): Promise<SessionEventWriterAppendResult> {
    const input = this.options.scopeForThread(envelope.workspaceId, envelope.sessionId, envelope.sessionThreadId);
    if (input === undefined) {
      return eventWriterUnavailable(envelope.sessionId, envelope.writeId);
    }
    const metadata = await this.metadataFactory({ tokenPath: this.options.tokenPath });
    const inputUncachedTokens = envelope.usage?.inputTokens ?? 0;
    const cacheReadTokens = envelope.usage?.cacheReadTokens ?? 0;
    const cacheWriteTokens = envelope.usage?.cacheWriteTokens ?? 0;
    const inputTokens = inputUncachedTokens + cacheReadTokens + cacheWriteTokens;
    const outputTokens = envelope.usage?.outputTokens ?? 0;
    const response = await writeRequestEnd(this.client, {
      scope: bridgeScope(input),
      runtimeWriteId: envelope.writeId,
      modelRequestId: envelope.modelRequestId,
      finishReason: envelope.finishReason,
      modelRequestStartEventId: envelope.modelRequestStartEventId,
      requestKind: envelope.requestKind ?? "",
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
    }, metadata);
    if (!bridgeAckAccepted(response.ack?.status)) {
      return eventWriterRejected(envelope.sessionId, envelope.writeId, response.ack?.errorCode);
    }
    const rescheduleDisposition = bridgeRescheduleDisposition(response.rescheduleDisposition);
    if (!rescheduleDisposition.ok) {
      return {
        ok: false,
        error: normalizeSessionEventWriterError({
          code: "schema_mismatch",
          sessionId: envelope.sessionId,
          writeId: envelope.writeId,
        }),
      };
    }
    if (envelope.reschedule !== undefined && rescheduleDisposition.value === undefined) {
      return {
        ok: false,
        error: normalizeSessionEventWriterError({
          code: "schema_mismatch",
          sessionId: envelope.sessionId,
          writeId: envelope.writeId,
        }),
      };
    }
    return {
      ok: true,
      writeId: response.ack?.runtimeWriteId ?? "",
      eventId: response.ack?.runtimeWriteId || envelope.writeId,
      processedAt: new Date().toISOString(),
      ...(rescheduleDisposition.value !== undefined ? { rescheduleDisposition: rescheduleDisposition.value } : {}),
    };
  }

  /** Persists the thread's idle transition and reports the supplied idle timestamp on success. */
  async finishIdle(envelope: SessionEventWriterFinishIdleEnvelope): Promise<SessionEventWriterAppendResult> {
    const input = this.options.scopeForThread(envelope.workspaceId, envelope.sessionId, envelope.sessionThreadId);
    if (input === undefined) {
      return eventWriterUnavailable(envelope.sessionId, envelope.writeId);
    }
    try {
      const metadata = await this.metadataFactory({ tokenPath: this.options.tokenPath });
      const response = await finishIdle(this.client, {
        scope: bridgeScope(input),
        runtimeWriteId: envelope.writeId,
        idleSince: envelope.idleSince,
        stopReasonJson: JSON.stringify(envelope.stopReason),
      }, metadata);
      if (!bridgeAckAccepted(response.ack?.status)) {
        return eventWriterRejected(envelope.sessionId, envelope.writeId, response.ack?.errorCode);
      }
      return {
        ok: true,
        writeId: response.ack?.runtimeWriteId ?? "",
        eventId: response.ack?.runtimeWriteId || envelope.writeId,
        processedAt: envelope.idleSince,
      };
    } catch (error) {
      return eventWriterTransportFailure(envelope.sessionId, envelope.writeId, error);
    }
  }

  /** Commits atomic runtime termination closeout for the active thread scope. */
  async commitRuntimeTermination(envelope: SessionEventWriterRuntimeTerminationEnvelope): Promise<SessionEventWriterAppendResult> {
    const input = this.options.scopeForThread(envelope.workspaceId, envelope.sessionId, envelope.sessionThreadId);
    if (input === undefined) {
      return eventWriterUnavailable(envelope.sessionId, envelope.writeId);
    }
    try {
      const metadata = await this.metadataFactory({ tokenPath: this.options.tokenPath });
      const response = await commitRuntimeTermination(this.client, {
        scope: bridgeScope(input),
        runtimeWriteId: envelope.writeId,
        failureJson: JSON.stringify(envelope.failure),
      }, metadata);
      if (!bridgeAckAccepted(response.ack?.status)) {
        return eventWriterRejected(envelope.sessionId, envelope.writeId, response.ack?.errorCode);
      }
      return {
        ok: true,
        writeId: response.ack?.runtimeWriteId ?? "",
        eventId: response.ack?.runtimeWriteId || envelope.writeId,
        processedAt: new Date().toISOString(),
      };
    } catch (error) {
      return eventWriterTransportFailure(envelope.sessionId, envelope.writeId, error);
    }
  }

}

function bridgeRescheduleDisposition(
  disposition: WriteRequestEndResponse["rescheduleDisposition"],
):
  | { readonly ok: true; readonly value?: {
      readonly status: "accepted";
      readonly attempt: number;
      readonly effectiveDeadline: string;
    } | {
      readonly status: "denied";
      readonly reason: "stale_terminal" | "attempt_mismatch" | "budget_exhausted";
      readonly attempt: number;
    } }
  | { readonly ok: false; readonly reason: string } {
  if (disposition === undefined) {
    return { ok: true };
  }
  if (disposition.status === "accepted") {
    if (!Number.isSafeInteger(disposition.attempt) || disposition.attempt <= 0 ||
      disposition.effectiveDeadline.length === 0 || !Number.isFinite(Date.parse(disposition.effectiveDeadline))) {
      return { ok: false, reason: "accepted reschedule disposition is malformed" };
    }
    return {
      ok: true,
      value: {
        status: "accepted",
        attempt: disposition.attempt,
        effectiveDeadline: disposition.effectiveDeadline,
      },
    };
  }
  if (disposition.status === "denied") {
    if (!Number.isSafeInteger(disposition.attempt) || disposition.attempt < 0 ||
      !["stale_terminal", "attempt_mismatch", "budget_exhausted"].includes(disposition.denialReason)) {
      return { ok: false, reason: "denied reschedule disposition is malformed" };
    }
    return {
      ok: true,
      value: {
        status: "denied",
        reason: disposition.denialReason as "stale_terminal" | "attempt_mismatch" | "budget_exhausted",
        attempt: disposition.attempt,
      },
    };
  }
  return { ok: false, reason: "reschedule disposition status is malformed" };
}

/** Configures durable internal-tool repair writes against active thread binding scopes. */
export interface BridgeAPIInternalToolRepairCommitterOptions {
  readonly address: string;
  readonly tokenPath: string;
  readonly scopeForThread: (workspaceId: string, sessionId: string, sessionThreadId: string) => RuntimeAcceptedInputState | undefined;
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

  /** Commits the repair's single tool part using the active scope for its session thread. */
  async commitInternalToolRepair(repair: RuntimeInternalToolRepairCommit): Promise<RuntimeMessageStoreWritePartResult> {
    const part = repair.message.parts[0];
    const input = this.options.scopeForThread(repair.workspaceId, repair.sessionId, repair.sessionThreadId);
    if (input === undefined || part === undefined) {
      return internalToolRepairStoreFailure("unavailable", repair.message.id, part?.id, repair.sessionId);
    }
    let metadata: Metadata;
    try {
      metadata = await this.metadataFactory({ tokenPath: this.options.tokenPath });
    } catch {
      return internalToolRepairStoreFailure("unavailable", repair.message.id, part.id, repair.sessionId);
    }
    let response: CommitInternalToolRepairResponse;
    try {
      response = await commitInternalToolRepair(this.client, {
        scope: bridgeScope(input),
        modelRequestId: repair.modelRequestId,
        modelToolCallId: repair.modelToolCallId,
        toolName: repair.toolName,
        dataJson: JSON.stringify(repair.message),
      }, metadata);
    } catch (error) {
      return internalToolRepairStoreFailure(bridgeStoreErrorCode(error), repair.message.id, part.id, repair.sessionId);
    }
    if (bridgeAckAccepted(response.ack?.status)) {
      return {
        ok: true,
        messageId: repair.message.id,
        partId: part.id,
        operation: "writePart",
      };
    }
    const code = response.ack?.errorCode === "internal_tool_repair_conflict" ? "constraint_violation" : "unavailable";
    return internalToolRepairStoreFailure(code, repair.message.id, part.id, repair.sessionId, response.ack?.errorCode);
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
    client.commitInputs(request, metadata, (error: ServiceError | null, response: CommitInputsResponse) => {
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

function runtimeDeclarationReceipt(receipt: BridgeDeclarationReceipt): RuntimeDeclarationReceipt {
  return {
    sessionThreadId: receipt.sessionThreadId,
    operationKind: receipt.operationKind,
    sourceKind: receipt.sourceKind,
    sourceId: receipt.sourceId,
    declarationDigest: receipt.declarationDigest,
    pendingAttachmentDelta: parsePendingAttachments(
      receipt.pendingAttachmentDeltaJson.map((item) => JSON.parse(item) as unknown),
    ) ?? [],
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

function bridgeScope(input: RuntimeThreadControlState): RuntimeScope {
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

function threadScopeKey(workspaceId: string, sessionId: string, sessionThreadId: string): string {
  return `${workspaceId}\x1f${sessionId}\x1f${sessionThreadId}`;
}

function interAgentCommitPayloadJson(input: Extract<RuntimeAcceptedInputState, { readonly kind: "inter_agent_message" }>): string {
  return JSON.stringify({
    delivery_id: input.deliveryId,
    source_thread_id: input.sourceThreadId,
    source_tool_use_event_id: input.sourceToolUseEventId,
    message: input.message,
    presentation: input.presentation ?? "push",
  });
}

function approvalReviewCommitPayloadJson(input: Extract<RuntimeAcceptedInputState, { readonly kind: "approval_review" }>): string {
  return JSON.stringify({
    review_id: input.reviewId,
    parent_thread_id: input.parentThreadId,
    target_model_tool_call_id: input.targetModelToolCallId,
    target_tool_name: input.targetToolName,
    prompt_items: input.promptItems,
    output_schema: input.outputSchemaJson,
  });
}

function bridgeAckAccepted(status: BridgeWriteStatus | undefined): boolean {
  return status === BridgeWriteStatus.BRIDGE_WRITE_STATUS_COMMITTED || status === BridgeWriteStatus.BRIDGE_WRITE_STATUS_DUPLICATE;
}

function bridgeCommitErrorRetryable(error: unknown): boolean {
  const code = typeof error === "object" && error !== null && "code" in error ? (error as { readonly code?: unknown }).code : undefined;
  return code !== status.INVALID_ARGUMENT && code !== status.ALREADY_EXISTS;
}

function bridgeStoreErrorCode(error: unknown): "unavailable" | "constraint_violation" {
  const code = typeof error === "object" && error !== null && "code" in error ? (error as { readonly code?: unknown }).code : undefined;
  return code === status.ALREADY_EXISTS ? "constraint_violation" : "unavailable";
}

function internalToolRepairStoreFailure(
  code: "unavailable" | "constraint_violation",
  messageId: string,
  partId: string | undefined,
  sessionId: string,
  constraint?: string | undefined,
): RuntimeMessageStoreWritePartResult {
  return {
    ok: false,
    error: normalizeRuntimeMessageStoreError({
      code,
      operation: "writePart",
      reason: "runtime_contract_validation",
      ...(constraint !== undefined && constraint !== "" ? { constraint } : {}),
      messageId,
      ...(partId !== undefined ? { partId } : {}),
      sessionId,
    }),
  };
}

function parseContextPayload(contextJson: string, input: RuntimeThreadControlState, agentMailPullFiltered = false): {
  readonly messages: readonly RuntimeMessage[];
  readonly threadContextPrefix?: ThreadContextPrefix | undefined;
  readonly thread: RuntimeAcceptedThreadMetadataState;
  readonly runtimeConfigPatch?: RuntimeConfigPatchState | undefined;
  readonly mcpManifests?: readonly RuntimeConfigPatchState[] | undefined;
  readonly pendingToolUses?: readonly RuntimeLoadedPendingToolUse[] | undefined;
  readonly backgroundTools?: readonly RuntimePreloadedBackgroundToolState[] | undefined;
  readonly pendingAttachments?: readonly ProviderRequestAttachment[] | undefined;
  readonly pendingAgentMail?: readonly RuntimeLoadedAgentMail[] | undefined;
} {
  try {
    const parsed = JSON.parse(contextJson) as Record<string, unknown>;
    if (!Array.isArray(parsed.messages)) {
      throw new Error("load context messages are malformed");
    }
    const messages = parsed.messages.map((message) => DurableRuntimeMessageSchema.parse(message));
    const threadContextPrefix = parseThreadContextPrefix(parsed.threadContextPrefix);
    const thread = parseThreadMetadata(parsed.thread);
    const runtimeConfigPatch = runtimeConfigPatchFromContextPayload(parsed, input);
    const mcpManifests = mcpManifestPatchesFromContextPayload(parsed.mcpManifests, input);
    const pendingToolUses = parsePendingToolUses(parsed.pendingToolUses);
    const backgroundTools = parseBackgroundTools(parsed.backgroundTools);
    const pendingAttachments = parsePendingAttachments(parsed.pendingAttachments);
    const pendingAgentMail = parsePendingAgentMail(parsed.pendingAgentMail, agentMailPullFiltered);
    return {
      messages,
      ...(threadContextPrefix !== undefined ? { threadContextPrefix } : {}),
      thread,
      ...(runtimeConfigPatch !== undefined ? { runtimeConfigPatch } : {}),
      ...(mcpManifests !== undefined ? { mcpManifests } : {}),
      ...(pendingToolUses !== undefined ? { pendingToolUses } : {}),
      ...(backgroundTools !== undefined ? { backgroundTools } : {}),
      ...(pendingAttachments !== undefined ? { pendingAttachments } : {}),
      ...(pendingAgentMail !== undefined ? { pendingAgentMail } : {}),
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
  const taskName = stringField(value, "taskName");
  return {
    role,
    visibility,
    status: status as RuntimeAcceptedThreadMetadataState["status"],
    agentType: agentType as NonNullable<RuntimeAcceptedThreadMetadataState["agentType"]>,
    ...(parentThreadId !== undefined ? { parentThreadId } : {}),
    ...(taskName !== undefined ? { taskName } : {}),
  };
}

function parsePendingAgentMail(value: unknown, pullFiltered: boolean): readonly RuntimeLoadedAgentMail[] | undefined {
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
      message: RuntimeMessageSchema.parse(item.message),
    };
  });
  if (!pullFiltered) {
    if (parsed.length > MailFetchMaxEnvelopes) {
      throw new Error("load context pendingAgentMail exceeds the envelope bound");
    }
    const bodyBytes = parsed.reduce(
      (total, mail) => total + new TextEncoder().encode(JSON.stringify(mail.message)).byteLength,
      0,
    );
    if (parsed.length > 1 && bodyBytes > MailFetchMaxBodyBytes) {
      throw new Error("load context pendingAgentMail exceeds the body-byte bound");
    }
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

function eventWriterUnavailable(sessionId: string, writeId: string): SessionEventWriterAppendResult {
  return {
    ok: false,
    error: normalizeSessionEventWriterError({
      code: "unavailable",
      sessionId,
      writeId,
    }),
  };
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
