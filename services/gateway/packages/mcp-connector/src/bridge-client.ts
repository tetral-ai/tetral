/**
 * @packageDocumentation
 *
 * Adapts MCP connector durability and manifest notifications to authenticated Agent Runtime
 * Bridge gRPC calls. Connector composition constructs these adapters, and the service uses them
 * to announce a changed manifest and to claim or commit a tool result around the external MCP
 * side effect. The module guards the inline-media transport ceiling, accepts only explicit Bridge
 * acknowledgements, keeps pending media out of stored responses, and reads committed refs-only
 * results back from Bridge's direct durable facts.
 */
import { credentials, status } from "@grpc/grpc-js";
import type { CallOptions, ChannelOptions, Metadata, ServiceError } from "@grpc/grpc-js";
import {
  AgentRuntimeBridgeServiceClient,
} from "@tetral/gateway-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import {
  McpErrorKind,
  McpRetryStatus,
  RunMcpToolStatus,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { RunMcpToolResponse } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type {
  ClaimMcpToolResultRequest,
  ClaimMcpToolResultResponse,
  CommitMcpToolResultRequest,
  CommitMcpToolResultResponse,
  McpManifestChangedRequest,
  McpManifestChangedResponse,
  RelinquishMcpToolResultRequest,
  RelinquishMcpToolResultResponse,
} from "@tetral/gateway-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import { buildOutboundBearerMetadata } from "./auth.js";
import {
  McpIdempotencyStaleCustodyError,
  parseStoredRunMcpToolResponse,
  serializePendingRunMcpToolResponse,
} from "./idempotency.js";
import type { ServiceAccountTokenConfig } from "./auth.js";
import type { IdempotencyClaim, IdempotencyKey, McpIdempotencyContext, McpIdempotencyStore, PendingStoredRunMcpToolResponse } from "./idempotency.js";
import { mcpGrpcClientKeepaliveOptions } from "./transport.js";
import { MCP_CLAIM_RPC_TIMEOUT_MS, MCP_COMMIT_RPC_TIMEOUT_MS } from "./phase-budgets.js";

export { MCP_CLAIM_RPC_TIMEOUT_MS, MCP_COMMIT_RPC_TIMEOUT_MS } from "./phase-budgets.js";

/** Bounds one Bridge manifest-change notification so retry scheduling cannot stall. */
export const MCP_MANIFEST_CHANGE_RPC_TIMEOUT_MS = 5_000;

const MCP_COMMIT_RETRY_BACKOFF_MS = [100, 300, 1_000] as const;

const MCP_COMMIT_PAYLOAD_REJECTION_RESULT: PendingStoredRunMcpToolResponse = {
  response: {
    status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_RUNTIME_ERROR,
    resultText: "MCP tool result could not be committed.",
    attachments: [],
    errorKind: McpErrorKind.MCP_ERROR_KIND_COMMIT_FAILED,
    retryStatus: McpRetryStatus.MCP_RETRY_STATUS_TERMINAL,
  },
  contentItems: 0,
  refreshTriggered: false,
};

/** Maximum send and receive size for Bridge commits that carry one bounded inline attachment. */
export const BridgeMcpCommitGrpcMessageBytes = 10 * 1024 * 1024 + 256 * 1024;

/** Returns symmetric channel ceilings for MCP claim and inline-media commit traffic. */
export function bridgeMcpCommitGrpcChannelOptions(): ChannelOptions {
  return {
    "grpc.max_receive_message_length": BridgeMcpCommitGrpcMessageBytes,
    "grpc.max_send_message_length": BridgeMcpCommitGrpcMessageBytes,
    ...mcpGrpcClientKeepaliveOptions(),
  };
}

/** Configures the authenticated Bridge client used to announce a changed MCP manifest. */
export interface BridgeAPIManifestChangeNotifierOptions {
  readonly address: string;
  readonly tokenPath: string;
  readonly metadataFactory?: (config: ServiceAccountTokenConfig) => Promise<Metadata>;
  readonly client?: BridgeManifestChangedClient;
  readonly timeoutMs?: number | undefined;
  readonly now?: (() => number) | undefined;
}

/** Describes the generated Bridge manifest-notification call used by the notifier adapter. */
export interface BridgeManifestChangedClient {
  mcpManifestChanged(
    request: McpManifestChangedRequest,
    metadata: Metadata,
    options: CallOptions,
    callback: (error: ServiceError | null, response: McpManifestChangedResponse) => void,
  ): unknown;
}

/** Describes the generated Bridge claim and commit calls used by the idempotency adapter. */
export interface BridgeMcpToolResultClient {
  claimMcpToolResult(
    request: ClaimMcpToolResultRequest,
    metadata: Metadata,
    options: CallOptions,
    callback: (error: ServiceError | null, response: ClaimMcpToolResultResponse) => void,
  ): unknown;
  commitMcpToolResult(
    request: CommitMcpToolResultRequest,
    metadata: Metadata,
    options: CallOptions,
    callback: (error: ServiceError | null, response: CommitMcpToolResultResponse) => void,
  ): unknown;
  relinquishMcpToolResult(
    request: RelinquishMcpToolResultRequest,
    metadata: Metadata,
    options: CallOptions,
    callback: (error: ServiceError | null, response: RelinquishMcpToolResultResponse) => void,
  ): unknown;
}

/** Reports a durable manifest acknowledgement or a bounded retry classification. */
export type BridgeAPIManifestChangeResult =
  | { readonly ok: true; readonly duplicate: boolean }
  | { readonly ok: false; readonly retryable: boolean; readonly code: string; readonly message: string };

type McpManifestChangeResult =
  | { readonly type: "committed" }
  | { readonly type: "duplicate" };

type McpToolClaimResult =
  | { readonly type: "acquired"; readonly executor: { readonly mcpServerName: string; readonly toolName: string; readonly inputJson: string } }
  | { readonly type: "already_completed"; readonly resultJson: string }
  | { readonly type: "in_flight" }
  | { readonly type: "stale" };

type McpToolCommitResult =
  | { readonly type: "committed"; readonly attachmentRef: string }
  | { readonly type: "duplicate"; readonly attachmentRef: string }
  | { readonly type: "stale" };

type McpToolRelinquishResult =
  | { readonly type: "relinquished" }
  | { readonly type: "duplicate" }
  | { readonly type: "stale" };

class BridgeMcpResultContractError extends Error {
  constructor(operation: string) {
    super(`${operation} returned an invalid result variant`);
    this.name = "BridgeMcpResultContractError";
  }
}

/**
 * Sends one authenticated manifest-change notification to Bridge.
 *
 * The connector service owns retry scheduling; this adapter performs one attempt, recognizes
 * committed and duplicate acknowledgements, and marks only token, transport, unavailable, and
 * deadline failures as retryable.
 */
export class BridgeAPIManifestChangeNotifier {
  private readonly client: BridgeManifestChangedClient;
  private readonly metadataFactory: (config: ServiceAccountTokenConfig) => Promise<Metadata>;
  private readonly timeoutMs: number;
  private readonly now: () => number;

  constructor(private readonly options: BridgeAPIManifestChangeNotifierOptions) {
    this.client = options.client ?? new AgentRuntimeBridgeServiceClient(options.address, credentials.createInsecure(), mcpGrpcClientKeepaliveOptions());
    this.metadataFactory = options.metadataFactory ?? buildOutboundBearerMetadata;
    this.timeoutMs = options.timeoutMs ?? MCP_MANIFEST_CHANGE_RPC_TIMEOUT_MS;
    this.now = options.now ?? Date.now;
  }

  async notify(input: McpManifestChangedRequest): Promise<BridgeAPIManifestChangeResult> {
    let metadata: Metadata;
    try {
      metadata = await this.metadataFactory({ tokenPath: this.options.tokenPath });
    } catch {
      return {
        ok: false,
        retryable: true,
        code: "bridge_token_unavailable",
        message: "mcp manifest change notification failed",
      };
    }
    let response: McpManifestChangedResponse;
    try {
      response = await mcpManifestChanged(this.client, input, metadata, {
        deadline: new Date(this.now() + this.timeoutMs),
      });
    } catch (error) {
      const serviceError = error as Partial<ServiceError>;
      return {
        ok: false,
        retryable: serviceError.code === undefined || serviceError.code === status.UNAVAILABLE || serviceError.code === status.DEADLINE_EXCEEDED,
        code: serviceError.code === undefined ? "bridge_unavailable" : `grpc_${serviceError.code}`,
        message: "mcp manifest change notification failed",
      };
    }
    try {
      const result = parseMcpManifestChangeResult(response);
      return { ok: true, duplicate: result.type === "duplicate" };
    } catch (error) {
      if (!(error instanceof BridgeMcpResultContractError)) {
        throw error;
      }
    }
    return {
      ok: false,
      retryable: false,
      code: "bridge_manifest_change_rejected",
      message: "mcp manifest change notification rejected",
    };
  }
}

/** Configures the authenticated Bridge client that owns durable MCP tool-result reservations. */
export interface BridgeAPIMcpToolResultIdempotencyStoreOptions {
  readonly address: string;
  readonly tokenPath: string;
  readonly metadataFactory?: (config: ServiceAccountTokenConfig) => Promise<Metadata>;
  readonly client?: BridgeMcpToolResultClient;
  readonly claimTimeoutMs?: number | undefined;
  readonly commitTimeoutMs?: number | undefined;
  readonly now?: (() => number) | undefined;
  readonly sleep?: ((delayMs: number) => Promise<void>) | undefined;
}

/**
 * Implements MCP tool-result idempotency through Bridge-owned claim and commit RPCs.
 *
 * Claims reserve one durable Tool target for an explicit execution attempt. Commits send pending
 * media bytes on the bounded inline leg; committed and duplicate ACKs return only the Bridge-generated
 * attachment ref, so a later claim read cannot reverse durable success. Claims and commits require
 * tenant, binding, and caller-pod context supplied by the connector service.
 */
export class BridgeAPIMcpToolResultIdempotencyStore implements McpIdempotencyStore {
  readonly #localClaims = new Set<string>();
  private readonly client: BridgeMcpToolResultClient;
  private readonly metadataFactory: (config: ServiceAccountTokenConfig) => Promise<Metadata>;
  private readonly claimTimeoutMs: number;
  private readonly commitTimeoutMs: number;
  private readonly now: () => number;
  private readonly sleep: (delayMs: number) => Promise<void>;

  constructor(private readonly options: BridgeAPIMcpToolResultIdempotencyStoreOptions) {
    this.client = options.client ?? new AgentRuntimeBridgeServiceClient(options.address, credentials.createInsecure(), bridgeMcpCommitGrpcChannelOptions());
    this.metadataFactory = options.metadataFactory ?? buildOutboundBearerMetadata;
    this.claimTimeoutMs = options.claimTimeoutMs ?? MCP_CLAIM_RPC_TIMEOUT_MS;
    this.commitTimeoutMs = options.commitTimeoutMs ?? MCP_COMMIT_RPC_TIMEOUT_MS;
    this.now = options.now ?? Date.now;
    this.sleep = options.sleep ?? sleep;
  }

  async claim(key: IdempotencyKey, context?: McpIdempotencyContext): Promise<IdempotencyClaim> {
    if (context === undefined) {
      throw new Error("mcp tool idempotency context is required");
    }
    let response: ClaimMcpToolResultResponse;
    try {
      response = await claimMcpToolResult(this.client, claimMcpToolResultRequest(key, context), await this.metadata(), {
        deadline: new Date(this.now() + this.claimTimeoutMs),
      });
    } catch (error) {
      if ((error as Partial<ServiceError>).code === status.ALREADY_EXISTS) {
        return { status: "conflict" };
      }
      throw error;
    }
    const result = parseMcpToolClaimResult(response);
    if (result.type === "already_completed") {
      const stored = parseStoredRunMcpToolResponse(result.resultJson);
      this.releaseLocalClaim(key, context);
      return { status: "replay", stored };
    }
    if (result.type === "acquired") {
      const localKey = localMcpClaimKey(key, context);
      if (this.#localClaims.has(localKey)) {
        return { status: "in_flight" };
      }
      this.#localClaims.add(localKey);
      return {
        status: "new",
        executor: result.executor,
      };
    }
    if (result.type === "in_flight") {
      return { status: "in_flight" };
    }
    return { status: "stale_custody" };
  }

  async store(key: IdempotencyKey, stored: PendingStoredRunMcpToolResponse, context?: McpIdempotencyContext) {
    if (context === undefined) {
      throw new Error("mcp tool idempotency context is required");
    }
    const request: CommitMcpToolResultRequest = freezeCommitRequest({
      scope: claimMcpToolResultRequest(key, context).scope,
      toolUseEventId: key.toolUseEventId,
      claimId: context.claimId,
      resultJson: serializePendingRunMcpToolResponse(stored),
      inlineMedia: stored.response.attachments.map((attachment) => ({
        data: new Uint8Array(attachment.data),
        mime: attachment.mime,
        suggestedFilename: attachment.suggestedFilename,
      })),
    });
    let activeRequest = request;
    let activeStored = stored;
    let committingPayloadRejection = false;
    let result: McpToolCommitResult;
    let committedResponse: RunMcpToolResponse | undefined;
    let outcomeUnknown = false;
    for (let attempt = 0; ; attempt++) {
      let metadata: Metadata;
      try {
        metadata = await this.metadata();
      } catch (error) {
        if (!outcomeUnknown) {
          this.releaseLocalClaim(key, context);
          throw error;
        }
        const delayIndex = Math.min(attempt, MCP_COMMIT_RETRY_BACKOFF_MS.length - 1);
        await this.sleep(MCP_COMMIT_RETRY_BACKOFF_MS[delayIndex] ?? 1_000);
        continue;
      }
      try {
        const response = await commitMcpToolResult(this.client, activeRequest, metadata, {
          deadline: new Date(this.now() + this.commitTimeoutMs),
        });
        try {
          result = parseMcpToolCommitResult(response);
          if (result.type !== "stale") {
            committedResponse = materializeCommittedMcpToolResponse(activeStored, result.attachmentRef);
          }
        } catch {
          // A malformed ACK is as ambiguous as a dropped ACK: Bridge may have
          // committed before a proxy or incompatible peer corrupted the
          // response. Keep the exact immutable request and converge through
          // duplicate replay; relinquishing this claim would invent certainty.
          outcomeUnknown = true;
          const delayIndex = Math.min(attempt, MCP_COMMIT_RETRY_BACKOFF_MS.length - 1);
          await this.sleep(MCP_COMMIT_RETRY_BACKOFF_MS[delayIndex] ?? 1_000);
          continue;
        }
        break;
      } catch (error) {
        const disposition = mcpCommitFailureDisposition(error);
        if (disposition === "result_payload_rejected" && !committingPayloadRejection) {
          activeRequest = freezeCommitRequest({
            ...request,
            resultJson: serializePendingRunMcpToolResponse(MCP_COMMIT_PAYLOAD_REJECTION_RESULT),
            inlineMedia: [],
          });
          activeStored = MCP_COMMIT_PAYLOAD_REJECTION_RESULT;
          committingPayloadRejection = true;
          outcomeUnknown = false;
          continue;
        }
        if (disposition === "stale_custody" || disposition === "result_payload_rejected") {
          this.releaseLocalClaim(key, context);
          throw new McpIdempotencyStaleCustodyError();
        }
        outcomeUnknown = true;
        const delayIndex = Math.min(attempt, MCP_COMMIT_RETRY_BACKOFF_MS.length - 1);
        await this.sleep(MCP_COMMIT_RETRY_BACKOFF_MS[delayIndex] ?? 1_000);
      }
    }
    if (result.type === "stale") {
      this.releaseLocalClaim(key, context);
      throw new McpIdempotencyStaleCustodyError();
    }
    this.releaseLocalClaim(key, context);
    return committedResponse!;
  }

  async fail(key: IdempotencyKey, context?: McpIdempotencyContext): Promise<void> {
    if (context === undefined) {
      return;
    }
    const request = relinquishMcpToolResultRequest(key, context);
    try {
      for (let attempt = 0; ; attempt += 1) {
        try {
          const response = await relinquishMcpToolResult(this.client, request, await this.metadata(), {
            deadline: new Date(this.now() + this.commitTimeoutMs),
          });
          parseMcpToolRelinquishResult(response);
          return;
        } catch (error) {
          if (!mcpRelinquishTransportRetryable(error) || attempt >= MCP_COMMIT_RETRY_BACKOFF_MS.length) {
            throw error;
          }
          await this.sleep(MCP_COMMIT_RETRY_BACKOFF_MS[attempt]!);
        }
      }
    } finally {
      this.releaseLocalClaim(key, context);
    }
  }

  private releaseLocalClaim(key: IdempotencyKey, context: McpIdempotencyContext): void {
    this.#localClaims.delete(localMcpClaimKey(key, context));
  }

  private async metadata(): Promise<Metadata> {
    return await this.metadataFactory({ tokenPath: this.options.tokenPath });
  }
}

type McpCommitFailureDisposition = "result_payload_rejected" | "stale_custody" | "outcome_unknown";

// Commit status is classified at the Bridge boundary: only request-shape
// rejection permits a canonical replacement while the same claim is tested by
// that replacement commit. Identity and custody statuses relinquish local
// ownership; every unclassified transport outcome replays immutable bytes.
function mcpCommitFailureDisposition(error: unknown): McpCommitFailureDisposition {
  switch ((error as Partial<ServiceError>).code) {
    case status.INVALID_ARGUMENT:
      return "result_payload_rejected";
    case status.ALREADY_EXISTS:
    case status.FAILED_PRECONDITION:
    case status.NOT_FOUND:
    case status.PERMISSION_DENIED:
    case status.UNAUTHENTICATED:
      return "stale_custody";
    default:
      return "outcome_unknown";
  }
}

function mcpRelinquishTransportRetryable(error: unknown): boolean {
  if (error instanceof BridgeMcpResultContractError) {
    return false;
  }
  const code = (error as Partial<ServiceError>).code;
  return code === undefined || code === status.UNAVAILABLE || code === status.DEADLINE_EXCEEDED ||
    code === status.INTERNAL || code === status.UNKNOWN;
}

function mcpManifestChanged(
  client: BridgeManifestChangedClient,
  request: McpManifestChangedRequest,
  metadata: Metadata,
  options: CallOptions,
): Promise<McpManifestChangedResponse> {
  return new Promise((resolve, reject) => {
    client.mcpManifestChanged(request, metadata, options, (error: ServiceError | null, response: McpManifestChangedResponse) => {
      if (error !== null) {
        reject(error);
        return;
      }
      resolve(response);
    });
  });
}

function claimMcpToolResult(
  client: BridgeMcpToolResultClient,
  request: ClaimMcpToolResultRequest,
  metadata: Metadata,
  options: CallOptions,
): Promise<ClaimMcpToolResultResponse> {
  return new Promise((resolve, reject) => {
    client.claimMcpToolResult(request, metadata, options, (error: ServiceError | null, response: ClaimMcpToolResultResponse) => {
      if (error !== null) {
        reject(error);
        return;
      }
      resolve(response);
    });
  });
}

function commitMcpToolResult(
  client: BridgeMcpToolResultClient,
  request: CommitMcpToolResultRequest,
  metadata: Metadata,
  options: CallOptions,
): Promise<CommitMcpToolResultResponse> {
  return new Promise((resolve, reject) => {
    client.commitMcpToolResult(request, metadata, options, (error: ServiceError | null, response: CommitMcpToolResultResponse) => {
      if (error !== null) {
        reject(error);
        return;
      }
      resolve(response);
    });
  });
}

function relinquishMcpToolResult(
  client: BridgeMcpToolResultClient,
  request: RelinquishMcpToolResultRequest,
  metadata: Metadata,
  options: CallOptions,
): Promise<RelinquishMcpToolResultResponse> {
  return new Promise((resolve, reject) => {
    client.relinquishMcpToolResult(request, metadata, options, (error: ServiceError | null, response: RelinquishMcpToolResultResponse) => {
      if (error !== null) {
        reject(error);
        return;
      }
      resolve(response);
    });
  });
}

function claimMcpToolResultRequest(key: IdempotencyKey, context: McpIdempotencyContext): ClaimMcpToolResultRequest {
  return {
    scope: {
      workspaceId: context.workspaceId,
      sessionId: context.sessionId,
      sessionThreadId: context.sessionThreadId,
      binding: {
        bindingId: context.bindingId,
        bindingGeneration: context.bindingGeneration,
        targetPodUid: context.runtimePodUid,
      },
    },
    toolUseEventId: key.toolUseEventId,
    claimId: context.claimId,
  };
}

function relinquishMcpToolResultRequest(key: IdempotencyKey, context: McpIdempotencyContext): RelinquishMcpToolResultRequest {
  return {
    scope: claimMcpToolResultRequest(key, context).scope,
    toolUseEventId: key.toolUseEventId,
    claimId: context.claimId,
  };
}

function localMcpClaimKey(key: IdempotencyKey, context: McpIdempotencyContext): string {
  return `${context.workspaceId}\u0000${context.sessionId}\u0000${context.sessionThreadId}\u0000${key.toolUseEventId}\u0000${context.claimId}`;
}

function parseMcpManifestChangeResult(response: McpManifestChangedResponse): McpManifestChangeResult {
  const variantCount = Number(response.committed !== undefined) + Number(response.duplicate !== undefined);
  if (variantCount !== 1) {
    throw new BridgeMcpResultContractError("McpManifestChanged");
  }
  return response.committed !== undefined ? { type: "committed" } : { type: "duplicate" };
}

function parseMcpToolClaimResult(response: ClaimMcpToolResultResponse): McpToolClaimResult {
  const variantCount = Number(response.acquired !== undefined) +
    Number(response.alreadyCompleted !== undefined) +
    Number(response.inFlight !== undefined) +
    Number(response.stale !== undefined);
  if (variantCount !== 1) {
    throw new BridgeMcpResultContractError("ClaimMcpToolResult");
  }
  if (response.acquired !== undefined) {
    return {
      type: "acquired",
      executor: {
        mcpServerName: response.acquired.mcpServerName,
        toolName: response.acquired.toolName,
        inputJson: response.acquired.inputJson,
      },
    };
  }
  if (response.alreadyCompleted !== undefined) {
    return { type: "already_completed", resultJson: response.alreadyCompleted.resultJson };
  }
  if (response.inFlight !== undefined) {
    return { type: "in_flight" };
  }
  return { type: "stale" };
}

function parseMcpToolCommitResult(response: CommitMcpToolResultResponse): McpToolCommitResult {
  const variantCount = Number(response.committed !== undefined) +
    Number(response.duplicate !== undefined) +
    Number(response.stale !== undefined);
  if (variantCount !== 1) {
    throw new BridgeMcpResultContractError("CommitMcpToolResult");
  }
  if (response.committed !== undefined) {
    return { type: "committed", attachmentRef: response.committed.attachmentRef };
  }
  if (response.duplicate !== undefined) {
    return { type: "duplicate", attachmentRef: response.duplicate.attachmentRef };
  }
  return { type: "stale" };
}

function materializeCommittedMcpToolResponse(
  stored: PendingStoredRunMcpToolResponse,
  attachmentRef: string,
): RunMcpToolResponse {
  const pendingAttachments = stored.response.attachments;
  if (pendingAttachments.length === 0) {
    if (attachmentRef !== "") {
      throw new BridgeMcpResultContractError("CommitMcpToolResult");
    }
    return {
      ...stored.response,
      attachments: [],
    };
  }
  if (pendingAttachments.length !== 1 || attachmentRef === "") {
    throw new BridgeMcpResultContractError("CommitMcpToolResult");
  }
  const attachment = pendingAttachments[0]!;
  return {
    ...stored.response,
    attachments: [{
      attachmentRef,
      mime: attachment.mime,
      sizeBytes: attachment.data.byteLength,
      suggestedFilename: attachment.suggestedFilename,
    }],
  };
}

function parseMcpToolRelinquishResult(response: RelinquishMcpToolResultResponse): McpToolRelinquishResult {
  const variantCount = Number(response.relinquished !== undefined) +
    Number(response.duplicate !== undefined) +
    Number(response.stale !== undefined);
  if (variantCount !== 1) {
    throw new BridgeMcpResultContractError("RelinquishMcpToolResult");
  }
  if (response.relinquished !== undefined) {
    return { type: "relinquished" };
  }
  if (response.duplicate !== undefined) {
    return { type: "duplicate" };
  }
  return { type: "stale" };
}

function freezeCommitRequest(request: CommitMcpToolResultRequest): CommitMcpToolResultRequest {
  for (const media of request.inlineMedia) {
    Object.freeze(media);
  }
  Object.freeze(request.inlineMedia);
  if (request.scope?.binding !== undefined) {
    Object.freeze(request.scope.binding);
  }
  if (request.scope !== undefined) {
    Object.freeze(request.scope);
  }
  return Object.freeze(request);
}

function sleep(delayMs: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, delayMs));
}
