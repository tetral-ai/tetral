/**
 * @packageDocumentation
 *
 * Resolves provider-request attachment references through the Agent Runtime
 * Bridge before provider lowering begins. The adapter presents projected bearer
 * metadata on each Bridge call, derives its scope from the validated provider
 * request and caller pod, and returns byte-bearing attachments together with
 * any identity-bearing per-reference rejections, or one bounded whole-request
 * error.
 *
 * Resolution preserves request order and verifies returned origins and MIME
 * types; file chunk responses must also match the requested byte count. File-backed
 * metadata is admitted before blob reads, file bytes stay within a per-request
 * envelope, and chunk reads must be exact. Deleted references remain local
 * rejections while transport outages and malformed responses fail the complete
 * resolution. `ProviderGatewayServiceShell` calls this adapter before invoking
 * the provider streamer; the adapter calls the generated Bridge API client and
 * supplies the resolved bytes to the lowering boundary without taking durable
 * ownership of them.
 */
import { credentials, status } from "@grpc/grpc-js";
import type {
  CallOptions,
  ClientUnaryCall,
  ChannelOptions,
  Metadata,
  ServiceError,
} from "@grpc/grpc-js";
import {
  AgentRuntimeBridgeServiceClient,
  FileAttachmentRejectionReason as BridgeFileAttachmentRejectionReason,
} from "@tetral/gateway-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type {
  ReadFileAttachmentChunkRequest,
  ReadFileAttachmentChunkResponse,
  ResolveFileAttachmentMetadataRequest,
  ResolveFileAttachmentMetadataResponse,
  ResolvedTransientAttachment,
  ResolveTransientAttachmentRequest,
  ResolveTransientAttachmentResponse,
  RuntimeScope,
} from "@tetral/gateway-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import { ProviderAttachmentRejectionReason } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type {
  ProviderAttachmentRejection,
  ProviderRequest,
  ProviderRequestAttachment,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { ResolvedProviderRequestAttachment } from "@tetral/gateway-lowering/src/request.js";
import type { ProviderErrorInput } from "@tetral/gateway-lowering/src/errors.js";
import { buildOutboundBearerMetadata } from "./auth.js";
import type { ProviderAttachmentResolveInput, ProviderAttachmentResolver } from "./service.js";
import type { ServiceAccountTokenConfig } from "./auth.js";
import { GatewayGrpcKeepaliveTimeMs, GatewayGrpcKeepaliveTimeoutMs } from "./bounds.js";

// Bridge attachment RPC channel cap: one maximum attachment payload (10 MiB) plus
// 64 KiB of protobuf/message headroom. The extra bytes cover the serialized
// envelope, not a second attachment payload.
/** Maximum message size accepted in either direction on the Bridge attachment channel. */
export const BridgeAttachmentGrpcMessageBytes = 10 * 1024 * 1024 + 64 * 1024;
// Maximum bytes ReadFileAttachmentChunk returns per offset-addressed call. readFile
// loops (offset, length) reads over one ref; there is no in-method retry, resume
// protocol, or streaming RPC. A short in-band response fails the whole resolution
// as retryable attachment_unavailable.
/** Maximum payload requested from the Bridge by one file attachment chunk read. */
export const FileAttachmentChunkBytes = 8 * 1024 * 1024;
// Metadata-gate envelope bounding the FILE-BACKED side (32 x 10 MiB), summed over
// a request's file-backed refs before any blob byte is read; a ref that would push
// the running total past it is dropped as over-envelope. Transient bytes use a
// separate per-ref limit, while both origins share the request's 32-item count;
// the combined resident bound therefore depends on that origin mix and is not
// twice this figure. The envelope is BINARY MiB (10,485,760 per unit); the upstream
// admission byte caps are DECIMAL MB (10,000,000) and the two
// are never normalized into each other. Reaching this gate is NOT text/plain-
// exclusive: the image (10,000,000 per file) and PDF (32,000,000 per request)
// admission caps are enforced independently yet share the single 32-attachment
// budget, so a mixed request crosses the envelope with zero text/plain — 31 images
// at 10,000,000 plus one PDF at 32,000,000 = 342,000,000 > 32 x 10,485,760 =
// 335,544,320 — and the over-envelope drop then fires on an image or PDF ref.
/** Maximum cumulative metadata-admitted size of file-backed attachments in one request. */
export const FileAttachmentEnvelopeBytes = 32 * 10 * 1024 * 1024;

/** Builds the symmetric gRPC message limits used by the Bridge attachment client. */
export function bridgeAttachmentGrpcChannelOptions(): ChannelOptions {
  return {
    "grpc.max_receive_message_length": BridgeAttachmentGrpcMessageBytes,
    "grpc.max_send_message_length": BridgeAttachmentGrpcMessageBytes,
    "grpc.keepalive_time_ms": GatewayGrpcKeepaliveTimeMs,
    "grpc.keepalive_timeout_ms": GatewayGrpcKeepaliveTimeoutMs,
    "grpc.keepalive_permit_without_calls": 0,
  };
}

/** Construction inputs for the Bridge-backed provider attachment resolver. */
export interface BridgeAPIAttachmentResolverOptions {
  readonly address: string;
  readonly tokenPath: string;
  readonly metadataFactory?: (config: ServiceAccountTokenConfig) => Promise<Metadata>;
  readonly client?: BridgeAttachmentClient;
}

/**
 * Unary Agent Runtime Bridge operations required to resolve transient and
 * file-backed attachment origins. Optional file methods permit transient-only
 * clients while causing file-backed resolution to fail closed.
 */
export interface BridgeAttachmentClient {
  resolveTransientAttachment(
    request: ResolveTransientAttachmentRequest,
    metadata: Metadata,
    options: CallOptions,
    callback: (error: ServiceError | null, response: ResolveTransientAttachmentResponse) => void,
  ): unknown;
  resolveFileAttachmentMetadata?(
    request: ResolveFileAttachmentMetadataRequest,
    metadata: Metadata,
    options: CallOptions,
    callback: (error: ServiceError | null, response: ResolveFileAttachmentMetadataResponse) => void,
  ): unknown;
  readFileAttachmentChunk?(
    request: ReadFileAttachmentChunkRequest,
    metadata: Metadata,
    options: CallOptions,
    callback: (error: ServiceError | null, response: ReadFileAttachmentChunkResponse) => void,
  ): unknown;
}

/**
 * Adapts scoped Bridge attachment RPCs to the provider service's attachment
 * resolver interface, preserving typed per-reference rejections separately
 * from whole-request failures.
 */
export class BridgeAPIAttachmentResolver implements ProviderAttachmentResolver {
  private readonly client: BridgeAttachmentClient;
  private readonly calls = new Map<ClientUnaryCall, Promise<void>>();
  private closing = false;
  private closed: Promise<void> | undefined;
  private readonly metadataFactory: (config: ServiceAccountTokenConfig) => Promise<Metadata>;

  constructor(private readonly options: BridgeAPIAttachmentResolverOptions) {
    this.client = options.client ?? new AgentRuntimeBridgeServiceClient(options.address, credentials.createInsecure(), bridgeAttachmentGrpcChannelOptions());
    this.metadataFactory = options.metadataFactory ?? buildOutboundBearerMetadata;
  }

  close(): Promise<void> {
    if (this.closed !== undefined) return this.closed;
    this.closing = true;
    this.closed = (async () => {
      const active = [...this.calls.entries()];
      for (const [call] of active) call.cancel();
      await Promise.all(active.map(([, join]) => join));
      if (this.client instanceof AgentRuntimeBridgeServiceClient)
        this.client.close();
    })();
    return this.closed;
  }

  private async call<Response>(
    method: keyof BridgeAttachmentClient,
    request: unknown,
    metadata: Metadata,
    deadline: number,
    signal?: AbortSignal,
  ): Promise<Response> {
    if (this.closing) throw new Error("Bridge attachment client closing");
    signal?.throwIfAborted();
    if (!Number.isFinite(deadline) || deadline <= Date.now())
      throw new Error("Provider attachment deadline exhausted");
    let call: ClientUnaryCall | undefined,
      settled = false,
      joined!: () => void;
    const join = new Promise<void>((resolve) => {
      joined = resolve;
    });
    const cancel = () => call?.cancel();
    try {
      return await new Promise<Response>((resolve, reject) => {
        const invoke = this.client[method] as unknown as (
          request: unknown,
          metadata: Metadata,
          options: CallOptions,
          callback: (error: ServiceError | null, response: Response) => void,
        ) => ClientUnaryCall;
        if (invoke === undefined)
          throw new Error("Bridge attachment method unavailable");
        call = invoke.call(
          this.client,
          request,
          metadata,
          { deadline },
          (error, response) => {
            settled = true;
            joined();
            if (error !== null) reject(error);
            else resolve(response);
          },
        );
        if (!settled) this.calls.set(call, join);
        signal?.addEventListener("abort", cancel, { once: true });
        if (signal?.aborted) cancel();
      });
    } finally {
      signal?.removeEventListener("abort", cancel);
      if (call !== undefined) this.calls.delete(call);
    }
  }

  /** Resolves all request attachments in order and validates every returned origin and byte count. */
  async resolve(input: ProviderAttachmentResolveInput): Promise<
    {
      readonly ok: true;
      readonly attachments: readonly ResolvedProviderRequestAttachment[];
      readonly rejections: readonly ProviderAttachmentRejection[];
    } |
    { readonly ok: false; readonly error: ProviderErrorInput }
  > {
    const deadline =
      input.deadline ?? Date.now() + (input.request.limits?.timeoutMs ?? 30000);
    let metadata: Metadata;
    try {
      metadata = await this.metadataFactory({ tokenPath: this.options.tokenPath });
    } catch {
      return { ok: false, error: attachmentBridgeError() };
    }

    const scope = bridgeScope(input.request, input.runtimePodUid);
    const fileAttachments = input.request.attachments.filter((attachment) => attachment.fileBacked !== undefined);
    let fileMetadata: ResolveFileAttachmentMetadataResponse["attachments"] = [];
    const fileRejectionReasons: Array<ProviderAttachmentRejectionReason | undefined> = [];
    if (fileAttachments.length > 0) {
      input.abortSignal?.throwIfAborted();
      try {
        const response = await this.call<ResolveFileAttachmentMetadataResponse>(
          "resolveFileAttachmentMetadata",
          {
            scope,
            attachments: fileAttachments.map(
              (attachment) => attachment.fileBacked!,
            ),
          },
          metadata,
          deadline,
          input.abortSignal,
        );
        fileMetadata = response.attachments;
      } catch (error) {
        return { ok: false, error: classifyAttachmentBridgeError(error) };
      }
      if (!validFileMetadata(fileAttachments, fileMetadata)) {
        return { ok: false, error: providerRequestInvalidError() };
      }
      let fileBytes = 0;
      for (const result of fileMetadata) {
        if (result.rejected !== undefined) {
          fileRejectionReasons.push(ProviderAttachmentRejectionReason.PROVIDER_ATTACHMENT_REJECTION_REASON_DELETED);
          continue;
        }
        const metadataEntry = result.metadata!;
        if (fileBytes + metadataEntry.sizeBytes > FileAttachmentEnvelopeBytes) {
          fileRejectionReasons.push(ProviderAttachmentRejectionReason.PROVIDER_ATTACHMENT_REJECTION_REASON_OVER_ENVELOPE);
          continue;
        }
        fileBytes += metadataEntry.sizeBytes;
        fileRejectionReasons.push(undefined);
      }
    }

    const resolved: ResolvedProviderRequestAttachment[] = [];
    const rejections: ProviderAttachmentRejection[] = [];
    let fileIndex = 0;
    for (const attachment of input.request.attachments) {
      input.abortSignal?.throwIfAborted();
      if (attachment.transient !== undefined) {
        const transientResult = await this.resolveTransient(
          scope,
          attachment,
          metadata,
          input.abortSignal,
          deadline,
        );
        if (!transientResult.ok) {
          return transientResult;
        }
        if (transientResult.type === "rejection") {
          rejections.push(transientResult.rejection);
          continue;
        }
        resolved.push(transientResult.attachment);
        continue;
      }
      const metadataEntry = fileMetadata[fileIndex]!;
      const rejectionReason = fileRejectionReasons[fileIndex];
      fileIndex += 1;
      if (rejectionReason !== undefined) {
        rejections.push({
          transientAttachmentRef: undefined,
          fileBacked: attachment.fileBacked,
          reason: rejectionReason,
        });
        continue;
      }
      const fileResult = await this.readFile(
        scope,
        attachment,
        metadataEntry.metadata!.sizeBytes,
        metadata,
        input.abortSignal,
        deadline,
      );
      if (!fileResult.ok) {
        return fileResult;
      }
      if (fileResult.type === "rejection") {
        rejections.push(fileResult.rejection);
        continue;
      }
      resolved.push({ ...attachment, data: fileResult.data });
    }
    return { ok: true, attachments: resolved, rejections };
  }

  private async resolveTransient(
    scope: RuntimeScope,
    attachment: ProviderRequestAttachment,
    metadata: Metadata,
    abortSignal: AbortSignal | undefined,
    deadline: number,
  ): Promise<
    {
      readonly ok: true;
      readonly type: "attachment";
      readonly attachment: ResolvedProviderRequestAttachment;
    } |
    {
      readonly ok: true;
      readonly type: "rejection";
      readonly rejection: ProviderAttachmentRejection;
    } |
    { readonly ok: false; readonly error: ProviderErrorInput }
  > {
    const transient = attachment.transient;
    if (transient === undefined) {
      return { ok: false, error: providerRequestInvalidError() };
    }
    let response: ResolveTransientAttachmentResponse;
    try {
      response = await this.call<ResolveTransientAttachmentResponse>(
        "resolveTransientAttachment",
        {
          scope,
          attachmentRef: transient.attachmentRef,
        },
        metadata,
        deadline,
        abortSignal,
      );
    } catch (error) {
      return { ok: false, error: classifyAttachmentBridgeError(error) };
    }
    abortSignal?.throwIfAborted();
    if (response.unavailable !== undefined && response.resolved === undefined) {
      return {
        ok: true,
        type: "rejection",
        rejection: {
          transientAttachmentRef: transient.attachmentRef,
          fileBacked: undefined,
          reason: ProviderAttachmentRejectionReason.PROVIDER_ATTACHMENT_REJECTION_REASON_DELETED,
        },
      };
    }
    if (
      response.resolved === undefined || response.unavailable !== undefined
      || response.resolved.data.length === 0
      || !sameTransientAttachmentMetadata(attachment, response.resolved)
    ) {
      return { ok: false, error: providerRequestInvalidError() };
    }
    return {
      ok: true,
      type: "attachment",
      attachment: {
        ...attachment,
        data: new Uint8Array(response.resolved.data),
      },
    };
  }

  private async readFile(
    scope: RuntimeScope,
    attachment: ProviderRequestAttachment,
    sizeBytes: number,
    metadata: Metadata,
    abortSignal: AbortSignal | undefined,
    deadline: number,
  ): Promise<
    { readonly ok: true; readonly type: "attachment"; readonly data: Uint8Array } |
    { readonly ok: true; readonly type: "rejection"; readonly rejection: ProviderAttachmentRejection } |
    { readonly ok: false; readonly error: ProviderErrorInput }
  > {
    if (attachment.fileBacked === undefined || !Number.isSafeInteger(sizeBytes) || sizeBytes < 0) {
      return { ok: false, error: providerRequestInvalidError() };
    }
    const data = new Uint8Array(sizeBytes);
    for (let offset = 0; offset < sizeBytes; offset += FileAttachmentChunkBytes) {
      abortSignal?.throwIfAborted();
      const length = Math.min(FileAttachmentChunkBytes, sizeBytes - offset);
      let response: ReadFileAttachmentChunkResponse;
      try {
        response = await this.call<ReadFileAttachmentChunkResponse>(
          "readFileAttachmentChunk",
          {
            scope,
            attachment: attachment.fileBacked,
            offset,
            length,
          },
          metadata,
          deadline,
          abortSignal,
        );
      } catch (error) {
        return { ok: false, error: classifyAttachmentBridgeError(error) };
      }
      if (response.rejected !== undefined && response.data === undefined) {
        if (
          response.rejected.reason !== BridgeFileAttachmentRejectionReason.FILE_ATTACHMENT_REJECTION_REASON_DELETED
          || response.rejected.attachment?.sourceEventId !== attachment.fileBacked.sourceEventId
          || response.rejected.attachment.fileId !== attachment.fileBacked.fileId
        ) {
          return { ok: false, error: providerRequestInvalidError() };
        }
        return {
          ok: true,
          type: "rejection",
          rejection: {
            transientAttachmentRef: undefined,
            fileBacked: attachment.fileBacked,
            reason: ProviderAttachmentRejectionReason.PROVIDER_ATTACHMENT_REJECTION_REASON_DELETED,
          },
        };
      }
      if (response.data === undefined || response.rejected !== undefined || response.data.length !== length) {
        return { ok: false, error: attachmentBridgeError() };
      }
      data.set(response.data, offset);
    }
    return { ok: true, type: "attachment", data };
  }
}

function bridgeScope(request: ProviderRequest, runtimePodUid: string): RuntimeScope {
  return {
    workspaceId: request.workspaceId,
    sessionId: request.sessionId,
    sessionThreadId: request.sessionThreadId,
    binding: {
      bindingId: request.bindingId,
      bindingGeneration: request.bindingGeneration,
      targetPodUid: runtimePodUid,
      runtimeProcessId: request.runtimeProcessId,
    },
  };
}

function sameTransientAttachmentMetadata(left: ProviderRequestAttachment, right: ResolvedTransientAttachment): boolean {
  return left.transient !== undefined &&
    right !== undefined &&
    left.transient.attachmentRef === right.attachmentRef &&
    left.mime === right.mime &&
    left.filename === right.filename &&
    left.transient.sourcePath === right.sourcePath &&
    left.transient.pageRange === right.pageRange &&
    left.transient.detail === right.detail;
}

function validFileMetadata(
  attachments: readonly ProviderRequestAttachment[],
  metadata: ResolveFileAttachmentMetadataResponse["attachments"],
): boolean {
  if (attachments.length !== metadata.length) {
    return false;
  }
  return attachments.every((attachment, index) => {
    const origin = attachment.fileBacked;
    const result = metadata[index];
    const resolvedOrigin = result?.metadata?.attachment ?? result?.rejected?.attachment;
    if (
      origin === undefined
      || resolvedOrigin?.sourceEventId !== origin.sourceEventId
      || resolvedOrigin.fileId !== origin.fileId
    ) {
      return false;
    }
    if (result?.rejected !== undefined) {
      return result.metadata === undefined &&
        result.rejected.reason === BridgeFileAttachmentRejectionReason.FILE_ATTACHMENT_REJECTION_REASON_DELETED;
    }
    return result?.metadata !== undefined &&
      result.rejected === undefined &&
      result.metadata.mime === attachment.mime &&
      Number.isSafeInteger(result.metadata.sizeBytes) &&
      result.metadata.sizeBytes >= 0;
  });
}

function attachmentBridgeError(): ProviderErrorInput {
  return {
    code: "attachment_unavailable",
    message: "Attachment bytes are not available to the provider request.",
    retryable: true,
    fatal: false,
    statusCode: 503,
  };
}

// Resolve outcome -> Gateway action. Per-ref domain outcomes ride IN-BAND as typed
// response fields and are handled in resolve()/resolveTransient()/readFile() (they
// never reach this classifier); transport statuses are mapped by the closed arms
// below, with every unrecognized error taking the fatal catch-all.
//
//   | resolve outcome                                   | Gateway action                              |
//   | ------------------------------------------------- | ------------------------------------------- |
//   | in-band rejected{deleted} (metadata or chunk arm) | drop that ref, report on attachment-        |
//   | / transient unavailable (expired/consumed)        | rejections, proceed with the valid subset   |
//   | INVALID_ARGUMENT (scope/origin/mime/malformed)    | reject the whole ProviderRequest as invalid |
//   | UNAVAILABLE / DEADLINE_EXCEEDED (outage)          | retryable whole-request error, zero drops   |
//   | any other status or error                         | fatal whole-request error, never retryable  |
//
// An in-band short read does not enter this classifier; readFile maps it to the
// retryable attachment_unavailable whole-request result.
function classifyAttachmentBridgeError(error: unknown): ProviderErrorInput {
  const code = (error as Partial<ServiceError>).code;
  if (code === status.INVALID_ARGUMENT) {
    return providerRequestInvalidError();
  }
  if (code === status.UNAVAILABLE || code === status.DEADLINE_EXCEEDED) {
    return attachmentBridgeError();
  }
  return {
    code: "provider_stream_error",
    message: "Attachment resolution failed.",
    retryable: false,
    fatal: true,
    statusCode: 500,
  };
}

function providerRequestInvalidError(): ProviderErrorInput {
  return {
    code: "provider_request_invalid",
    message: "File attachment request is invalid.",
    retryable: false,
    fatal: true,
    statusCode: 400,
  };
}
