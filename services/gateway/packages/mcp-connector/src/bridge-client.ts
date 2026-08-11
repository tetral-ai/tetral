/**
 * @packageDocumentation
 *
 * Adapts MCP connector durability and manifest notifications to authenticated Agent Runtime
 * Bridge gRPC calls. Connector composition constructs these adapters, and the service uses them
 * to announce a changed manifest and to claim or commit a tool result around the external MCP
 * side effect. The module guards the inline-media transport ceiling, accepts only explicit Bridge
 * acknowledgements, keeps pending media out of stored responses, and returns the refs-only result
 * produced by Bridge.
 */
import { createHash } from "node:crypto";
import { credentials, status } from "@grpc/grpc-js";
import type { CallOptions, ChannelOptions, Metadata, ServiceError } from "@grpc/grpc-js";
import {
  AgentRuntimeBridgeServiceClient,
  BridgeWriteStatus,
  ReceiptApplicationDisposition,
} from "@tetral/gateway-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import { canonicalRunToolJSON } from "@tetral/gateway-protocol/src/run-tool-canonical-json.js";
import type {
  ClaimMcpToolResultRequest,
  ClaimMcpToolResultResponse,
  CommitMcpToolResultRequest,
  CommitMcpToolResultResponse,
  McpManifestChangedRequest,
  McpManifestChangedResponse,
} from "@tetral/gateway-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import { buildOutboundBearerMetadata } from "./auth.js";
import {
  InMemoryMcpIdempotencyStore,
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

const MCP_MANIFEST_TOO_LARGE_DETAILS = "mcp manifest tools exceed the accepted byte limit";

const MCP_COMMIT_RETRY_BACKOFF_MS = [100, 300, 1_000] as const;

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
}

/** Reports a durable manifest acknowledgement or a bounded retry classification. */
export type BridgeAPIManifestChangeResult =
  | { readonly ok: true; readonly duplicate: boolean }
  | { readonly ok: false; readonly retryable: boolean; readonly code: string; readonly message: string };

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
      if (
        serviceError.code === status.RESOURCE_EXHAUSTED
        && serviceError.details === MCP_MANIFEST_TOO_LARGE_DETAILS
      ) {
        // Bridge uses ResourceExhausted only after durably settling an over-cap
        // manifest as unready with Queue custody. The terminal status is its ACK.
        return { ok: true, duplicate: false };
      }
      return {
        ok: false,
        retryable: serviceError.code === undefined || serviceError.code === status.UNAVAILABLE || serviceError.code === status.DEADLINE_EXCEEDED,
        code: serviceError.code === undefined ? "bridge_unavailable" : `grpc_${serviceError.code}`,
        message: "mcp manifest change notification failed",
      };
    }
    if (response.ack?.status === BridgeWriteStatus.BRIDGE_WRITE_STATUS_COMMITTED) {
      return { ok: true, duplicate: false };
    }
    if (response.ack?.status === BridgeWriteStatus.BRIDGE_WRITE_STATUS_DUPLICATE) {
      return { ok: true, duplicate: true };
    }
    return {
      ok: false,
      retryable: false,
      code: response.ack?.errorCode || "bridge_manifest_change_rejected",
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
 * Claims reserve canonical request identity before the external tool runs. Commits send pending
 * media bytes on the bounded inline leg, parse Bridge's refs-only response, and mirror completed
 * state locally through defensive replay copies. Claims and commits require tenant, binding,
 * server, tool, and caller-pod context supplied by the connector service.
 */
export class BridgeAPIMcpToolResultIdempotencyStore implements McpIdempotencyStore {
  readonly #local = new InMemoryMcpIdempotencyStore();
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
    if (response.ack?.status === BridgeWriteStatus.BRIDGE_WRITE_STATUS_DUPLICATE) {
      if (response.materializationHandle === undefined || response.materializationHandle.length === 0) {
        throw new Error("mcp tool replay is missing its materialization handle");
      }
      const disposition = mcpClaimReplayDisposition(response, claimMcpToolResultRequest(key, context), context);
      if (disposition === undefined) {
        throw new Error("mcp tool replay returned malformed declaration receipt");
      }
      if (disposition === "stale_custody") {
        return { status: "stale_custody" };
      }
      const stored = parseStoredRunMcpToolResponse(response.resultJson);
      stored.response.materializationHandle = response.materializationHandle;
      this.#local.storeCompleted(key, stored, context);
      return { status: "replay", stored };
    }
    if (response.ack?.status === BridgeWriteStatus.BRIDGE_WRITE_STATUS_COMMITTED) {
      const localClaim = this.#local.claim(key, context);
      if (localClaim.status === "new") {
        return { status: "new" };
      }
      await this.#local.fail(key, context);
      return localClaim.status === "conflict" ? { status: "conflict" } : { status: "in_flight" };
    }
    if (response.ack?.status === BridgeWriteStatus.BRIDGE_WRITE_STATUS_REJECTED && response.ack.errorCode === "mcp_claim_in_flight") {
      return { status: "in_flight" };
    }
    return { status: "conflict" };
  }

  async store(key: IdempotencyKey, stored: PendingStoredRunMcpToolResponse, context?: McpIdempotencyContext) {
    if (context === undefined) {
      throw new Error("mcp tool idempotency context is required");
    }
    const request: CommitMcpToolResultRequest = freezeCommitRequest({
      ...claimMcpToolResultRequest(key, context),
      resultJson: serializePendingRunMcpToolResponse(stored),
      inlineMedia: stored.response.attachments.map((attachment) => ({
        data: new Uint8Array(attachment.data),
        mime: attachment.mime,
        suggestedFilename: attachment.suggestedFilename,
      })),
    });
    let response: CommitMcpToolResultResponse;
    let outcomeUnknown = false;
    for (let attempt = 0; ; attempt++) {
      let metadata: Metadata;
      try {
        metadata = await this.metadata();
      } catch (error) {
        if (!outcomeUnknown) {
          await this.#local.fail(key, context);
          throw error;
        }
        const delayIndex = Math.min(attempt, MCP_COMMIT_RETRY_BACKOFF_MS.length - 1);
        await this.sleep(MCP_COMMIT_RETRY_BACKOFF_MS[delayIndex] ?? 1_000);
        continue;
      }
      try {
        response = await commitMcpToolResult(this.client, request, metadata, {
          deadline: new Date(this.now() + this.commitTimeoutMs),
        });
        break;
      } catch {
        outcomeUnknown = true;
        const delayIndex = Math.min(attempt, MCP_COMMIT_RETRY_BACKOFF_MS.length - 1);
        await this.sleep(MCP_COMMIT_RETRY_BACKOFF_MS[delayIndex] ?? 1_000);
      }
    }
    if (
      response.ack?.status !== BridgeWriteStatus.BRIDGE_WRITE_STATUS_DUPLICATE &&
      response.ack?.status !== BridgeWriteStatus.BRIDGE_WRITE_STATUS_COMMITTED
    ) {
      await this.#local.fail(key, context);
      throw new Error("mcp tool idempotency commit rejected");
    }
    const disposition = mcpCommitDisposition(response, request, context);
    if (disposition === undefined) {
      await this.#local.fail(key, context);
      throw new Error("mcp tool commit returned malformed declaration receipt");
    }
    if (disposition === "stale_custody") {
      await this.#local.fail(key, context);
      throw new McpIdempotencyStaleCustodyError();
    }
    if (response.materializationHandle === undefined || response.materializationHandle.length === 0) {
      await this.#local.fail(key, context);
      throw new Error("mcp tool commit is missing its materialization handle");
    }
    const committed = parseStoredRunMcpToolResponse(response.refsOnlyResultJson);
    committed.response.materializationHandle = response.materializationHandle;
    return this.#local.storeCompleted(key, committed, context);
  }

  async fail(key: IdempotencyKey, context?: McpIdempotencyContext): Promise<void> {
    await this.#local.fail(key, context);
  }

  private async metadata(): Promise<Metadata> {
    return await this.metadataFactory({ tokenPath: this.options.tokenPath });
  }
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

function claimMcpToolResultRequest(key: IdempotencyKey, context: McpIdempotencyContext): ClaimMcpToolResultRequest {
  return {
    scope: {
      requestId: context.requestId,
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
    normalizedInputHash: key.normalizedInputHash,
    mcpServerName: context.mcpServerName,
    toolName: context.toolName,
    inputJson: context.inputJson,
  };
}

function mcpCommitDisposition(
  response: CommitMcpToolResultResponse,
  request: CommitMcpToolResultRequest,
  context: McpIdempotencyContext,
): "current_custody" | "stale_custody" | undefined {
  const declaration = response.declaration;
  const receipt = declaration?.receipts[0];
  if (
    declaration === undefined ||
    declaration.receipts.length !== 1 ||
    receipt === undefined ||
    !validMcpMaterializationReceipt(
      receipt,
      request.scope?.sessionThreadId ?? "",
      request.toolUseEventId,
      request.mcpServerName,
      stableMcpMaterializationOperationId(request),
      mcpMaterializationDeclarationDigest(request),
      response.refsOnlyResultJson,
    )
  ) {
    return undefined;
  }
  return declarationDisposition(declaration, context);
}

function mcpClaimReplayDisposition(
  response: ClaimMcpToolResultResponse,
  request: ClaimMcpToolResultRequest,
  context: McpIdempotencyContext,
): "current_custody" | "stale_custody" | undefined {
  const declaration = response.declaration;
  const receipt = declaration?.receipts[0];
  if (
    declaration === undefined ||
    declaration.receipts.length !== 1 ||
    receipt === undefined ||
    receipt.declarationDigest.length === 0 ||
    !validMcpMaterializationReceipt(
      receipt,
      request.scope?.sessionThreadId ?? "",
      request.toolUseEventId,
      request.mcpServerName,
      stableMcpMaterializationOperationId(request),
      receipt.declarationDigest,
      response.resultJson,
    )
  ) {
    return undefined;
  }
  return declarationDisposition(declaration, context);
}

function declarationDisposition(
  declaration: NonNullable<CommitMcpToolResultResponse["declaration"]>,
  context: McpIdempotencyContext,
): "current_custody" | "stale_custody" | undefined {
  if (
    declaration.applicationDisposition === ReceiptApplicationDisposition.RECEIPT_APPLICATION_DISPOSITION_CURRENT_CUSTODY &&
    declaration.observedBindingId === context.bindingId &&
    declaration.observedBindingGeneration === context.bindingGeneration
  ) {
    return "current_custody";
  }
  if (declaration.applicationDisposition === ReceiptApplicationDisposition.RECEIPT_APPLICATION_DISPOSITION_STALE_CUSTODY) {
    return "stale_custody";
  }
  return undefined;
}

function validMcpMaterializationReceipt(
  receipt: NonNullable<CommitMcpToolResultResponse["declaration"]>["receipts"][number],
  sessionThreadId: string,
  toolUseEventId: string,
  mcpServerName: string,
  operationId: string,
  declarationDigest: string,
  refsOnlyResultJson: string,
): boolean {
  let stored;
  try {
    stored = parseStoredRunMcpToolResponse(refsOnlyResultJson);
  } catch {
    return false;
  }
  if (
    receipt.sessionThreadId !== sessionThreadId ||
    receipt.operationKind !== "commit_mcp_tool_result" ||
    receipt.sourceKind !== "mcp_tool_execution" ||
    receipt.operationId !== operationId ||
    receipt.declarationDigest !== declarationDigest
  ) {
    return false;
  }
  if (
    receipt.events.length !== 0 ||
    receipt.messages.length !== 0 ||
    receipt.pendingAttachmentDeltaJson.length !== stored.response.attachments.length ||
    receipt.interruptToolProjections.length !== 0 ||
    receipt.prefixConsumptions.length !== 0 ||
    receipt.requestReschedule !== undefined ||
    receipt.childLifecycle.length !== 0 ||
    receipt.idleCloseout !== undefined ||
    receipt.compactedThroughMessageSequence !== undefined
  ) {
    return false;
  }
  return receipt.pendingAttachmentDeltaJson.every((encoded, index) => {
    const attachment = stored.response.attachments[index];
    if (attachment === undefined) {
      return false;
    }
    const expected = {
      origin: {
        transient: {
          attachmentRef: attachment.attachmentRef,
          sourceToolUseEventId: toolUseEventId,
          sourcePath: `mcp:${mcpServerName}/${attachment.suggestedFilename}`,
          detail: "auto",
        },
      },
      mime: attachment.mime,
      filename: attachment.suggestedFilename,
    };
    try {
      return canonicalRunToolJSON(encoded) === canonicalRunToolJSON(JSON.stringify(expected));
    } catch {
      return false;
    }
  });
}

function stableMcpMaterializationOperationId(
  request: Pick<ClaimMcpToolResultRequest, "toolUseEventId" | "normalizedInputHash">,
): string {
  const encoder = new TextEncoder();
  const parts = [
    "mcp_tool_execution",
    request.toolUseEventId,
    request.normalizedInputHash,
  ].map((part) => encoder.encode(part));
  const framed = new Uint8Array(parts.reduce((total, part) => total + 4 + part.byteLength, 0));
  const view = new DataView(framed.buffer);
  let offset = 0;
  for (const part of parts) {
    view.setUint32(offset, part.byteLength, false);
    offset += 4;
    framed.set(part, offset);
    offset += part.byteLength;
  }
  return `stid_${createHash("sha256").update(framed).digest("hex")}`;
}

/** Returns the SHA-256 digest Bridge must echo for one MCP materialization declaration. */
export function mcpMaterializationDeclarationDigest(request: CommitMcpToolResultRequest): string {
  const inlineMedia = request.inlineMedia.map((media) => ({
      content_sha256: createHash("sha256").update(media.data).digest("hex"),
      mime: media.mime,
      suggested_filename: media.suggestedFilename,
    }));
  const declaration = `{
    "inline_media":${JSON.stringify(inlineMedia)},
    "input":${canonicalRunToolJSON(request.inputJson)},
    "mcp_server_name":${JSON.stringify(request.mcpServerName)},
    "normalized_input_hash":${JSON.stringify(request.normalizedInputHash)},
    "operation_kind":"commit_mcp_tool_result",
    "result":${canonicalRunToolJSON(request.resultJson)},
    "session_thread_id":${JSON.stringify(request.scope?.sessionThreadId ?? "")},
    "tool_name":${JSON.stringify(request.toolName)},
    "tool_use_event_id":${JSON.stringify(request.toolUseEventId)}
  }`;
  return createHash("sha256").update(canonicalRunToolJSON(declaration), "utf8").digest("hex");
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
