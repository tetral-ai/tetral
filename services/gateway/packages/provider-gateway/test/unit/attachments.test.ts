import { describe, expect, test } from "bun:test";
import { Metadata, status } from "@grpc/grpc-js";
import { ProviderAttachmentRejectionReason } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { FileAttachmentRejectionReason as BridgeFileAttachmentRejectionReason } from "@tetral/gateway-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import {
  BridgeAPIAttachmentResolver,
  BridgeAttachmentGrpcMessageBytes,
  FileAttachmentEnvelopeBytes,
  bridgeAttachmentGrpcChannelOptions,
} from "../../src/attachments.js";
import { GatewayGrpcKeepaliveTimeMs, GatewayGrpcKeepaliveTimeoutMs } from "../../src/bounds.js";
import { validFileBackedProviderAttachment, validProviderAttachment, validProviderRequest } from "./fixtures.js";
import type {
  ReadFileAttachmentChunkRequest,
  ReadFileAttachmentChunkResponse,
  FileAttachmentMetadataResult,
  FileAttachmentPair,
  ResolveFileAttachmentMetadataRequest,
  ResolveFileAttachmentMetadataResponse,
  ResolveTransientAttachmentRequest,
  ResolveTransientAttachmentResponse,
} from "@tetral/gateway-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type { ServiceError } from "@grpc/grpc-js";

describe("BridgeAPIAttachmentResolver", () => {
  test("configures the Bridge channel for the 10 MiB attachment rail", () => {
    expect(bridgeAttachmentGrpcChannelOptions()).toEqual({
      "grpc.max_receive_message_length": BridgeAttachmentGrpcMessageBytes,
      "grpc.max_send_message_length": BridgeAttachmentGrpcMessageBytes,
      "grpc.keepalive_time_ms": GatewayGrpcKeepaliveTimeMs,
      "grpc.keepalive_timeout_ms": GatewayGrpcKeepaliveTimeoutMs,
      "grpc.keepalive_permit_without_calls": 0,
    });
    expect(BridgeAttachmentGrpcMessageBytes).toBeGreaterThan(10 * 1024 * 1024);
  });
  test("resolves refs through Bridge with scoped binding identity", async () => {
    const client = new RecordingBridgeAttachmentClient((request) =>
      resolvedTransientAttachment(request, {
        mime: "image/png",
        filename: "image.png",
        sourcePath: "/tmp/image.png",
        data: new Uint8Array([1, 2, 3]),
      })
    );
    const resolver = new BridgeAPIAttachmentResolver({
      address: "bridge.test:9090",
      tokenPath: "/var/run/bridge/token",
      client,
      metadataFactory: async ({ tokenPath }) => {
        expect(tokenPath).toBe("/var/run/bridge/token");
        const metadata = new Metadata();
        metadata.set("authorization", "bearer gateway-token");
        return metadata;
      },
    });

    const result = await resolver.resolve({
      request: validProviderRequest({ attachments: [validProviderAttachment()] }),
      runtimePodUid: "pod_uid_runtime",
    });

    expect(result).toEqual({
      ok: true,
      attachments: [{
        transient: {
          attachmentRef: "att_1",
          sourcePath: "/tmp/image.png",
          pageRange: "",
          detail: "auto",
        },
        fileBacked: undefined,
        mime: "image/png",
        filename: "image.png",
        data: new Uint8Array([1, 2, 3]),
      }],
      rejections: [],
    });
    expect(client.requests[0]).toMatchObject({
      attachmentRef: "att_1",
      scope: {
        workspaceId: "wksp_1",
        sessionId: "sesn_1",
        sessionThreadId: "thrd_1",
        binding: {
          bindingId: "bind_1",
          bindingGeneration: 42,
          targetPodUid: "pod_uid_runtime",
        },
      },
    });
  });

  test("preflights every file-backed ref before reading deterministic 8 MiB chunks", async () => {
    const eightMiB = 8 * 1024 * 1024;
    const first = validFileBackedProviderAttachment({
      fileBacked: { sourceEventId: "sevt_user_1", fileId: "file_1" },
      mime: "text/plain",
      filename: "first.txt",
    });
    const second = validFileBackedProviderAttachment({
      fileBacked: { sourceEventId: "sevt_user_2", fileId: "file_2" },
      mime: "application/pdf",
      filename: "second.pdf",
    });
    const client = new RecordingMixedBridgeAttachmentClient([
      fileMetadataResult(first.fileBacked!, first.mime, first.filename, eightMiB + 3),
      fileMetadataResult(second.fileBacked!, second.mime, second.filename, 2),
    ]);
    const resolver = new BridgeAPIAttachmentResolver({
      address: "bridge.test:9090",
      tokenPath: "/var/run/bridge/token",
      client,
      metadataFactory: async () => new Metadata(),
    });

    const result = await resolver.resolve({
      request: validProviderRequest({ attachments: [first, second] }),
      runtimePodUid: "pod_uid_runtime",
    });

    expect(client.callOrder[0]).toBe("metadata:file_1,file_2");
    expect(client.callOrder.slice(1)).toEqual([
      `chunk:file_1:0:${eightMiB}`,
      `chunk:file_1:${eightMiB}:3`,
      "chunk:file_2:0:2",
    ]);
    expect(result).toEqual({
      ok: true,
      attachments: [
        { ...first, data: new Uint8Array(eightMiB + 3).fill(1) },
        { ...second, data: new Uint8Array(2).fill(2) },
      ],
      rejections: [],
    });
  });

  test("drops an over-envelope file in derivation order and reads the surviving prefix", async () => {
    const first = validFileBackedProviderAttachment({
      fileBacked: { sourceEventId: "sevt_user_1", fileId: "file_1" },
    });
    const second = validFileBackedProviderAttachment({
      fileBacked: { sourceEventId: "sevt_user_2", fileId: "file_2" },
    });
    const client = new RecordingMixedBridgeAttachmentClient([
      fileMetadataResult(first.fileBacked!, first.mime, first.filename, 1),
      fileMetadataResult(second.fileBacked!, second.mime, second.filename, FileAttachmentEnvelopeBytes),
    ]);
    const resolver = new BridgeAPIAttachmentResolver({
      address: "bridge.test:9090",
      tokenPath: "/var/run/bridge/token",
      client,
      metadataFactory: async () => new Metadata(),
    });

    const result = await resolver.resolve({
      request: validProviderRequest({ attachments: [first, second] }),
      runtimePodUid: "pod_uid_runtime",
    });

    expect(result).toEqual({
      ok: true,
      attachments: [{ ...first, data: new Uint8Array([1]) }],
      rejections: [{
        transientAttachmentRef: undefined,
        fileBacked: second.fileBacked,
        reason: ProviderAttachmentRejectionReason.PROVIDER_ATTACHMENT_REJECTION_REASON_OVER_ENVELOPE,
      }],
    });
    expect(client.callOrder).toEqual(["metadata:file_1,file_2", "chunk:file_1:0:1"]);
  });

  test("drops a deleted file and reads the other file from the same metadata batch", async () => {
    const first = validFileBackedProviderAttachment({
      fileBacked: { sourceEventId: "sevt_user_1", fileId: "file_1" },
    });
    const deleted = validFileBackedProviderAttachment({
      fileBacked: { sourceEventId: "sevt_user_2", fileId: "file_2" },
    });
    const client = new RecordingMixedBridgeAttachmentClient([
      fileMetadataResult(first.fileBacked!, first.mime, first.filename, 1),
      fileDeletedResult(deleted.fileBacked!),
    ]);
    const resolver = new BridgeAPIAttachmentResolver({
      address: "bridge.test:9090",
      tokenPath: "/var/run/bridge/token",
      client,
      metadataFactory: async () => new Metadata(),
    });

    const result = await resolver.resolve({
      request: validProviderRequest({ attachments: [first, deleted] }),
      runtimePodUid: "pod_uid_runtime",
    });

    expect(result).toEqual({
      ok: true,
      attachments: [{ ...first, data: new Uint8Array([1]) }],
      rejections: [{
        transientAttachmentRef: undefined,
        fileBacked: deleted.fileBacked,
        reason: ProviderAttachmentRejectionReason.PROVIDER_ATTACHMENT_REJECTION_REASON_DELETED,
      }],
    });
    expect(client.callOrder).toEqual(["metadata:file_1,file_2", "chunk:file_1:0:1"]);
  });

  test("fails closed on a short chunk without reading the next offset", async () => {
    const eightMiB = 8 * 1024 * 1024;
    const attachment = validFileBackedProviderAttachment();
    const client = new RecordingMixedBridgeAttachmentClient([
      fileMetadataResult(attachment.fileBacked!, attachment.mime, attachment.filename, eightMiB + 1),
    ], -1);
    const resolver = new BridgeAPIAttachmentResolver({
      address: "bridge.test:9090",
      tokenPath: "/var/run/bridge/token",
      client,
      metadataFactory: async () => new Metadata(),
    });

    const result = await resolver.resolve({
      request: validProviderRequest({ attachments: [attachment] }),
      runtimePodUid: "pod_uid_runtime",
    });

    expect(result).toMatchObject({ ok: false, error: { code: "attachment_unavailable" } });
    expect(client.callOrder).toEqual([
      "metadata:file_1",
      `chunk:file_1:0:${eightMiB}`,
    ]);
  });

  test("classifies mismatched transient metadata as an invalid provider request", async () => {
    const client = new RecordingBridgeAttachmentClient((request) =>
      resolvedTransientAttachment(request, {
        mime: "application/pdf",
        filename: "wrong.pdf",
        sourcePath: "/tmp/image.png",
        data: new Uint8Array([1]),
      })
    );
    const resolver = new BridgeAPIAttachmentResolver({
      address: "bridge.test:9090",
      tokenPath: "/var/run/bridge/token",
      client,
      metadataFactory: async () => new Metadata(),
    });

    const result = await resolver.resolve({
      request: validProviderRequest({ attachments: [validProviderAttachment()] }),
      runtimePodUid: "pod_uid_runtime",
    });

    expect(result).toMatchObject({
      ok: false,
      error: { code: "provider_request_invalid", retryable: false, fatal: true },
    });
  });

  test("classifies temporary Bridge failures as retryable attachment errors", async () => {
    const client = {
      resolveTransientAttachment(
        _request: ResolveTransientAttachmentRequest,
        _metadata: Metadata,
        callback: (error: ServiceError | null, response: ResolveTransientAttachmentResponse) => void,
      ): unknown {
        callback(Object.assign(new Error("bridge unavailable"), { code: status.UNAVAILABLE }) as ServiceError, {} as ResolveTransientAttachmentResponse);
        return {};
      },
    };
    const resolver = new BridgeAPIAttachmentResolver({
      address: "bridge.test:9090",
      tokenPath: "/var/run/bridge/token",
      client,
      metadataFactory: async () => new Metadata(),
    });

    const result = await resolver.resolve({
      request: validProviderRequest({ attachments: [validProviderAttachment()] }),
      runtimePodUid: "pod_uid_runtime",
    });

    expect(result).toMatchObject({
      ok: false,
      error: { code: "attachment_unavailable", retryable: true, fatal: false, statusCode: 503 },
    });
  });

  test("classifies an invalid file attachment pair as a provider request defect", async () => {
    const attachment = validFileBackedProviderAttachment();
    const client = new RecordingMixedBridgeAttachmentClient([]);
    client.metadataError = Object.assign(new Error("file attachment source event is invalid"), {
      code: status.INVALID_ARGUMENT,
    }) as ServiceError;
    const resolver = new BridgeAPIAttachmentResolver({
      address: "bridge.test:9090",
      tokenPath: "/var/run/bridge/token",
      client,
      metadataFactory: async () => new Metadata(),
    });

    const result = await resolver.resolve({
      request: validProviderRequest({ attachments: [attachment] }),
      runtimePodUid: "pod_uid_runtime",
    });

    expect(result).toMatchObject({
      ok: false,
      error: { code: "provider_request_invalid", retryable: false, fatal: true, statusCode: 400 },
    });
    expect(client.callOrder).toEqual(["metadata:file_1"]);
  });

  test("drops expired or consumed transient refs without terminalizing the provider request", async () => {
    const client = new RecordingBridgeAttachmentClient(() => ({
      resolved: undefined,
      unavailable: {},
    }));
    const resolver = new BridgeAPIAttachmentResolver({
      address: "bridge.test:9090",
      tokenPath: "/var/run/bridge/token",
      client,
      metadataFactory: async () => new Metadata(),
    });

    const result = await resolver.resolve({
      request: validProviderRequest({ attachments: [validProviderAttachment()] }),
      runtimePodUid: "pod_uid_runtime",
    });

    expect(result).toEqual({
      ok: true,
      attachments: [],
      rejections: [{
        transientAttachmentRef: validProviderAttachment().transient?.attachmentRef,
        fileBacked: undefined,
        reason: ProviderAttachmentRejectionReason.PROVIDER_ATTACHMENT_REJECTION_REASON_DELETED,
      }],
    });
  });

  test("drops a file deleted after metadata preflight and keeps the valid subset", async () => {
    const deleted = validFileBackedProviderAttachment({
      fileBacked: { sourceEventId: "sevt_deleted_after_metadata", fileId: "file_deleted_after_metadata" },
    });
    const valid = validFileBackedProviderAttachment({
      fileBacked: { sourceEventId: "sevt_valid_after_metadata", fileId: "file_valid_after_metadata" },
    });
    const client = new RecordingMixedBridgeAttachmentClient([
      fileMetadataResult(deleted.fileBacked!, deleted.mime, deleted.filename, 1),
      fileMetadataResult(valid.fileBacked!, valid.mime, valid.filename, 1),
    ]);
    client.chunkRejectedFileID = deleted.fileBacked!.fileId;
    const resolver = new BridgeAPIAttachmentResolver({
      address: "bridge.test:9090",
      tokenPath: "/var/run/bridge/token",
      client,
      metadataFactory: async () => new Metadata(),
    });

    const result = await resolver.resolve({
      request: validProviderRequest({ attachments: [deleted, valid] }),
      runtimePodUid: "pod_uid_runtime",
    });

    expect(result).toEqual({
      ok: true,
      attachments: [{ ...valid, data: new Uint8Array([2]) }],
      rejections: [{
        transientAttachmentRef: undefined,
        fileBacked: deleted.fileBacked,
        reason: ProviderAttachmentRejectionReason.PROVIDER_ATTACHMENT_REJECTION_REASON_DELETED,
      }],
    });
    expect(client.callOrder).toEqual([
      "metadata:file_deleted_after_metadata,file_valid_after_metadata",
      "chunk:file_deleted_after_metadata:0:1",
      "chunk:file_valid_after_metadata:0:1",
    ]);
  });
});

class RecordingBridgeAttachmentClient {
  readonly requests: ResolveTransientAttachmentRequest[] = [];

  constructor(private readonly response: (request: ResolveTransientAttachmentRequest) => ResolveTransientAttachmentResponse) {}

  resolveTransientAttachment(
    request: ResolveTransientAttachmentRequest,
    _metadata: Metadata,
    callback: (error: ServiceError | null, response: ResolveTransientAttachmentResponse) => void,
  ): unknown {
    this.requests.push(request);
    callback(null, this.response(request));
    return {};
  }
}

class RecordingMixedBridgeAttachmentClient extends RecordingBridgeAttachmentClient {
  readonly callOrder: string[] = [];
  metadataError: ServiceError | undefined;
  chunkRejectedFileID: string | undefined;

  constructor(
    private readonly fileMetadata: ResolveFileAttachmentMetadataResponse["attachments"],
    private readonly chunkLengthDelta = 0,
  ) {
    super(() => {
      throw new Error("unexpected transient resolution");
    });
  }

  resolveFileAttachmentMetadata(
    request: ResolveFileAttachmentMetadataRequest,
    _metadata: Metadata,
    callback: (error: ServiceError | null, response: ResolveFileAttachmentMetadataResponse) => void,
  ): unknown {
    this.callOrder.push(`metadata:${request.attachments.map((attachment) => attachment.fileId).join(",")}`);
    if (this.metadataError !== undefined) {
      callback(this.metadataError, {} as ResolveFileAttachmentMetadataResponse);
      return {};
    }
    callback(null, { attachments: this.fileMetadata });
    return {};
  }

  readFileAttachmentChunk(
    request: ReadFileAttachmentChunkRequest,
    _metadata: Metadata,
    callback: (error: ServiceError | null, response: ReadFileAttachmentChunkResponse) => void,
  ): unknown {
    this.callOrder.push(`chunk:${request.attachment?.fileId}:${request.offset}:${request.length}`);
    if (request.attachment?.fileId === this.chunkRejectedFileID) {
      callback(null, {
        data: undefined,
        rejected: {
          attachment: request.attachment,
          reason: BridgeFileAttachmentRejectionReason.FILE_ATTACHMENT_REJECTION_REASON_DELETED,
        },
      });
      return {};
    }
    const fill = request.attachment?.fileId === "file_1" ? 1 : 2;
    callback(null, {
      data: new Uint8Array(Math.max(0, request.length + this.chunkLengthDelta)).fill(fill),
      rejected: undefined,
    });
    return {};
  }
}

function resolvedTransientAttachment(
  request: ResolveTransientAttachmentRequest,
  input: {
    readonly mime: string;
    readonly filename: string;
    readonly sourcePath: string;
    readonly data: Uint8Array;
  },
): ResolveTransientAttachmentResponse {
  return {
    resolved: {
      attachmentRef: request.attachmentRef,
      mime: input.mime,
      filename: input.filename,
      sourcePath: input.sourcePath,
      pageRange: "",
      detail: "auto",
      data: input.data,
    },
    unavailable: undefined,
  };
}

function fileMetadataResult(
  attachment: FileAttachmentPair,
  mime: string,
  filename: string,
  sizeBytes: number,
): FileAttachmentMetadataResult {
  return {
    metadata: { attachment, mime, filename, sizeBytes },
    rejected: undefined,
  };
}

function fileDeletedResult(attachment: FileAttachmentPair): FileAttachmentMetadataResult {
  return {
    metadata: undefined,
    rejected: {
      attachment,
      reason: BridgeFileAttachmentRejectionReason.FILE_ATTACHMENT_REJECTION_REASON_DELETED,
    },
  };
}
