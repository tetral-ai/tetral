import { createHash } from "node:crypto";
import { describe, expect, test } from "bun:test";
import { Metadata, Server, ServerCredentials, status } from "@grpc/grpc-js";
import type { ServiceError } from "@grpc/grpc-js";
import {
  AgentRuntimeBridgeServiceService,
  BridgeWriteStatus,
  ReceiptApplicationDisposition,
} from "@tetral/gateway-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type { AgentRuntimeBridgeServiceServer } from "@tetral/gateway-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import {
  BridgeAPIMcpToolResultIdempotencyStore,
  BridgeAPIManifestChangeNotifier,
  BridgeMcpCommitGrpcMessageBytes,
  MCP_CLAIM_RPC_TIMEOUT_MS,
  MCP_COMMIT_RPC_TIMEOUT_MS,
  MCP_MANIFEST_CHANGE_RPC_TIMEOUT_MS,
  bridgeMcpCommitGrpcChannelOptions,
} from "../../src/bridge-client.js";
import { McpGrpcKeepaliveTimeMs, McpGrpcKeepaliveTimeoutMs } from "../../src/transport.js";
import type {
  ClaimMcpToolResultRequest,
  ClaimMcpToolResultResponse,
  CommitMcpToolResultRequest,
  CommitMcpToolResultResponse,
  DeclarationResponse,
  McpManifestChangedRequest,
  McpManifestChangedResponse,
} from "@tetral/gateway-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type { CallOptions } from "@grpc/grpc-js";
import { RunMcpToolStatus } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { canonicalRunToolJSON } from "@tetral/gateway-protocol/src/run-tool-canonical-json.js";
import {
  InMemoryMcpIdempotencyStore,
  McpIdempotencyStaleCustodyError,
} from "../../src/idempotency.js";

describe("BridgeAPIManifestChangeNotifier", () => {
  test("admits the bounded MCP inline-media commit on both gRPC directions", () => {
    expect(BridgeMcpCommitGrpcMessageBytes).toBe(10 * 1024 * 1024 + 256 * 1024);
    expect(bridgeMcpCommitGrpcChannelOptions()).toEqual({
      "grpc.max_receive_message_length": BridgeMcpCommitGrpcMessageBytes,
      "grpc.max_send_message_length": BridgeMcpCommitGrpcMessageBytes,
      "grpc.keepalive_time_ms": McpGrpcKeepaliveTimeMs,
      "grpc.keepalive_timeout_ms": McpGrpcKeepaliveTimeoutMs,
      "grpc.keepalive_permit_without_calls": 0,
    });
  });

  test("sends manifest change notifications with outbound service-account metadata", async () => {
    const client = new RecordingBridgeClient({
      ack: { status: BridgeWriteStatus.BRIDGE_WRITE_STATUS_COMMITTED, runtimeInputId: "runtime_config_update:mcp_manifest:github:etag_1", runtimeWriteId: "", errorCode: "" },
    });
    const notifier = new BridgeAPIManifestChangeNotifier({
      address: "bridge.tetral-system.svc.cluster.local:9090",
      tokenPath: "/token",
      client,
      metadataFactory: async () => {
        const metadata = new Metadata();
        metadata.set("authorization", "bearer projected-token");
        return metadata;
      },
    });

    await expect(notifier.notify({
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      mcpServerName: "github",
      manifestEtag: "etag_1",
    })).resolves.toEqual({ ok: true, duplicate: false });

    expect(client.calls).toEqual([{
      request: {
        workspaceId: "wksp_1",
        sessionId: "sesn_1",
        mcpServerName: "github",
        manifestEtag: "etag_1",
      },
      authorization: ["bearer projected-token"],
    }]);
  });

  test("classifies manifest notification token and transport failures", async () => {
    const request = {
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      mcpServerName: "github",
      manifestEtag: "etag_1",
    };
    const tokenFailure = new BridgeAPIManifestChangeNotifier({
      address: "bridge.tetral-system.svc.cluster.local:9090",
      tokenPath: "/token",
      client: new RecordingBridgeClient({ ack: undefined }),
      metadataFactory: async () => { throw new Error("unavailable"); },
    });
    await expect(tokenFailure.notify(request)).resolves.toMatchObject({
      ok: false,
      retryable: true,
      code: "bridge_token_unavailable",
    });

    for (const testCase of [
      { name: "transport without status", code: undefined, retryable: true, resultCode: "bridge_unavailable" },
      { name: "unavailable", code: status.UNAVAILABLE, retryable: true, resultCode: `grpc_${status.UNAVAILABLE}` },
      { name: "deadline exceeded", code: status.DEADLINE_EXCEEDED, retryable: true, resultCode: `grpc_${status.DEADLINE_EXCEEDED}` },
      { name: "defined terminal status", code: status.INVALID_ARGUMENT, retryable: false, resultCode: `grpc_${status.INVALID_ARGUMENT}` },
    ] as const) {
      const notifier = new BridgeAPIManifestChangeNotifier({
        address: "bridge.tetral-system.svc.cluster.local:9090",
        tokenPath: "/token",
        client: new FailingBridgeClient(testCase.code),
        metadataFactory: async () => new Metadata(),
      });
      await expect(notifier.notify(request), testCase.name).resolves.toMatchObject({
        ok: false,
        retryable: testCase.retryable,
        code: testCase.resultCode,
      });
    }
  });

  test("bounds a hung manifest RPC and classifies its deadline for retry", async () => {
    const server = new Server();
    server.addService(AgentRuntimeBridgeServiceService, {
      mcpManifestChanged: () => undefined,
    } as unknown as AgentRuntimeBridgeServiceServer);
    const port = await bindLocalServer(server);
    try {
      const notifier = new BridgeAPIManifestChangeNotifier({
        address: `127.0.0.1:${port}`,
        tokenPath: "/token",
        metadataFactory: async () => new Metadata(),
        timeoutMs: 20,
      });

      await expect(notifier.notify({
        workspaceId: "wksp_1",
        sessionId: "sesn_1",
        mcpServerName: "github",
        manifestEtag: "etag_1",
      })).resolves.toMatchObject({
        ok: false,
        retryable: true,
        code: `grpc_${status.DEADLINE_EXCEEDED}`,
      });
      expect(MCP_MANIFEST_CHANGE_RPC_TIMEOUT_MS).toBe(5_000);
    } finally {
      server.forceShutdown();
    }
  });

  test("treats a defined Bridge ACK rejection as terminal", async () => {
    const notifier = new BridgeAPIManifestChangeNotifier({
      address: "bridge.tetral-system.svc.cluster.local:9090",
      tokenPath: "/token",
      client: new RecordingBridgeClient({
        ack: { status: BridgeWriteStatus.BRIDGE_WRITE_STATUS_REJECTED, runtimeInputId: "", runtimeWriteId: "", errorCode: "manifest_rejected" },
      }),
      metadataFactory: async () => new Metadata(),
    });
    await expect(notifier.notify({
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      mcpServerName: "github",
      manifestEtag: "etag_1",
    })).resolves.toEqual({
      ok: false,
      retryable: false,
      code: "manifest_rejected",
      message: "mcp manifest change notification rejected",
    });
  });
});

describe("BridgeAPIMcpToolResultIdempotencyStore", () => {
  test("keeps the same tool-use event id isolated across thread-scoped local caches", () => {
    const store = new InMemoryMcpIdempotencyStore();
    const key = { toolUseEventId: "sevt_shared", normalizedInputHash: "hash_shared" };
    const main = mcpIdempotencyContext();
    const child = { ...main, sessionThreadId: "thrd_child" };

    expect(store.claim(key, main)).toEqual({ status: "new" });
    expect(store.claim(key, child)).toEqual({ status: "new" });
    store.store(key, {
      response: { status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED, resultText: "main", attachments: [] },
      contentItems: 1,
      refreshTriggered: false,
    }, main);
    store.store(key, {
      response: { status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED, resultText: "child", attachments: [] },
      contentItems: 1,
      refreshTriggered: false,
    }, child);

    expect(store.claim(key, main)).toMatchObject({ status: "replay", stored: { response: { resultText: "main" } } });
    expect(store.claim(key, child)).toMatchObject({ status: "replay", stored: { response: { resultText: "child" } } });
  });

  test("returns Bridge's durable materialization handle with the refs-only response", async () => {
    const client = new MaterializationBridgeClient();
    const store = new BridgeAPIMcpToolResultIdempotencyStore({
      address: "bridge.tetral-system.svc.cluster.local:9090",
      tokenPath: "/token",
      client,
      metadataFactory: async () => new Metadata(),
    });
    const key = { toolUseEventId: "sevt_tool_1", normalizedInputHash: "hash_1" };
    const context = mcpIdempotencyContext();

    await expect(store.claim(key, context)).resolves.toEqual({ status: "new" });
    await expect(store.store(key, {
      response: {
        status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED,
        resultText: "ok",
        attachments: [],
      },
      contentItems: 1,
      refreshTriggered: false,
    }, context)).resolves.toMatchObject({
      resultText: "ok",
      materializationHandle: "sevt_tool_1",
    });
  });

  test("rejects a committed MCP result whose declaration names stale custody", async () => {
    const store = new BridgeAPIMcpToolResultIdempotencyStore({
      address: "bridge.tetral-system.svc.cluster.local:9090",
      tokenPath: "/token",
      client: new MaterializationBridgeClient({ staleCustody: true }),
      metadataFactory: async () => new Metadata(),
    });
    const key = { toolUseEventId: "sevt_tool_1", normalizedInputHash: "hash_1" };
    const context = mcpIdempotencyContext();

    await expect(store.claim(key, context)).resolves.toEqual({ status: "new" });
    await expect(store.store(key, {
      response: {
        status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED,
        resultText: "ok",
        attachments: [],
      },
      contentItems: 1,
      refreshTriggered: false,
    }, context)).rejects.toBeInstanceOf(McpIdempotencyStaleCustodyError);
  });

  test("retries an unknown commit outcome with the exact frozen declaration", async () => {
    const client = new UnknownThenReplayBridgeClient();
    const delays: number[] = [];
    const store = new BridgeAPIMcpToolResultIdempotencyStore({
      address: "bridge.tetral-system.svc.cluster.local:9090",
      tokenPath: "/token",
      client,
      metadataFactory: async () => new Metadata(),
      sleep: async (delayMs) => {
        delays.push(delayMs);
      },
    });
    const key = { toolUseEventId: "sevt_tool_1", normalizedInputHash: "hash_1" };
    const context = mcpIdempotencyContext();

    await expect(store.claim(key, context)).resolves.toEqual({ status: "new" });
    await expect(store.store(key, {
      response: {
        status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED,
        resultText: "ok",
        attachments: [],
      },
      contentItems: 1,
      refreshTriggered: false,
    }, context)).resolves.toMatchObject({
      resultText: "ok",
      materializationHandle: "sevt_tool_1",
    });

    expect(delays).toEqual([100]);
    expect(client.commitRequests).toHaveLength(2);
    expect(client.commitRequests[0]).toBe(client.commitRequests[1]);
    expect(client.commitRequests[0]).toEqual(client.commitRequests[1]);
  });

  test("classifies a durable replay observed under stale custody without local application", async () => {
    const store = new BridgeAPIMcpToolResultIdempotencyStore({
      address: "bridge.tetral-system.svc.cluster.local:9090",
      tokenPath: "/token",
      client: new StaleClaimReplayBridgeClient(),
      metadataFactory: async () => new Metadata(),
    });

    await expect(store.claim(
      { toolUseEventId: "sevt_tool_1", normalizedInputHash: "hash_1" },
      mcpIdempotencyContext(),
    )).resolves.toEqual({ status: "stale_custody" });
  });

  test("rejects an attachment receipt whose delta differs from the refs-only result", async () => {
    const store = new BridgeAPIMcpToolResultIdempotencyStore({
      address: "bridge.tetral-system.svc.cluster.local:9090",
      tokenPath: "/token",
      client: new MalformedAttachmentReceiptBridgeClient(),
      metadataFactory: async () => new Metadata(),
    });
    const key = { toolUseEventId: "sevt_tool_1", normalizedInputHash: "hash_1" };
    const context = mcpIdempotencyContext();

    await expect(store.claim(key, context)).resolves.toEqual({ status: "new" });
    await expect(store.store(key, {
      response: {
        status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED,
        resultText: "image",
        attachments: [{
          data: Uint8Array.from([1, 2, 3]),
          mime: "image/png",
          suggestedFilename: "plot.png",
        }],
      },
      contentItems: 1,
      refreshTriggered: false,
    }, context)).rejects.toThrow("mcp tool commit returned malformed declaration receipt");
  });

  test("bounds a hung Claim RPC before any external tool execution can begin", async () => {
    const server = new Server();
    server.addService(AgentRuntimeBridgeServiceService, {
      claimMcpToolResult: () => undefined,
    } as unknown as AgentRuntimeBridgeServiceServer);
    const port = await bindLocalServer(server);
    try {
      const store = new BridgeAPIMcpToolResultIdempotencyStore({
        address: `127.0.0.1:${port}`,
        tokenPath: "/token",
        metadataFactory: async () => new Metadata(),
        claimTimeoutMs: 20,
      });

      await expect(store.claim({
        toolUseEventId: "sevt_tool_1",
        normalizedInputHash: "hash_1",
      }, mcpIdempotencyContext())).rejects.toMatchObject({ code: status.DEADLINE_EXCEEDED });
      expect(MCP_CLAIM_RPC_TIMEOUT_MS).toBe(10_000);
    } finally {
      server.forceShutdown();
    }
  });

  test("keeps an unknown commit receipt awaiting past the capped backoff without rebuilding the declaration", async () => {
    const client = new RepeatedUnknownThenReplayBridgeClient();
    const sleepDelays: number[] = [];
    const attachmentBytes = Uint8Array.from([1, 2, 3]);
    let metadataGeneration = 0;
    const store = new BridgeAPIMcpToolResultIdempotencyStore({
      address: "bridge.tetral-system.svc.cluster.local:9090",
      tokenPath: "/token",
      client,
      metadataFactory: async () => {
        const metadata = new Metadata();
        metadata.set("authorization", `bearer token_${++metadataGeneration}`);
        return metadata;
      },
      claimTimeoutMs: 321,
      commitTimeoutMs: 1_234,
      now: () => 1_000,
      sleep: async (delayMs) => {
        sleepDelays.push(delayMs);
        attachmentBytes[0] = 9;
      },
    });
    const key = { toolUseEventId: "sevt_tool_1", normalizedInputHash: "hash_1" };
    const context = {
      requestId: "req_1",
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      sessionThreadId: "thrd_1",
      bindingId: "bind_1",
      bindingGeneration: 1,
      runtimePodUid: "pod_1",
      mcpServerName: "github",
      toolName: "create_issue",
      inputJson: "{}",
    };

    await expect(store.claim(key, context)).resolves.toEqual({ status: "new" });
    await expect(store.store(key, {
      response: {
        status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED,
        resultText: "ok",
        attachments: [{
          data: attachmentBytes,
          mime: "image/png",
          suggestedFilename: "plot.png",
        }],
      },
      contentItems: 1,
      refreshTriggered: false,
    }, context)).resolves.toMatchObject({
      status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED,
      resultText: "ok",
    });

    expect(client.claimOptions).toEqual([{ deadline: new Date(1_321) }]);
    expect(client.commitOptions).toHaveLength(5);
    expect(client.commitOptions).toEqual(
      Array.from({ length: 5 }, () => ({ deadline: new Date(2_234) })),
    );
    expect(new Set(client.commitRequests).size).toBe(1);
    expect(client.commitAttachmentSnapshots).toEqual(
      Array.from({ length: 5 }, () => [1, 2, 3]),
    );
    expect(client.commitAuthorizations).toEqual([
      ["bearer token_2"],
      ["bearer token_3"],
      ["bearer token_4"],
      ["bearer token_5"],
      ["bearer token_6"],
    ]);
    expect(sleepDelays).toEqual([100, 300, 1_000, 1_000]);
    expect(MCP_COMMIT_RPC_TIMEOUT_MS).toBe(10_000);
  });

});

class RecordingBridgeClient {
  readonly calls: Array<{
    readonly request: McpManifestChangedRequest;
    readonly authorization: unknown;
  }> = [];

  constructor(private readonly response: McpManifestChangedResponse) {}

  mcpManifestChanged(
    request: McpManifestChangedRequest,
    metadata: Metadata,
    _options: CallOptions,
    callback: (error: null, response: McpManifestChangedResponse) => void,
  ) {
    this.calls.push({ request, authorization: metadata.get("authorization") });
    callback(null, this.response);
  }
}

async function bindLocalServer(server: Server): Promise<number> {
  return await new Promise<number>((resolve, reject) => {
    server.bindAsync("127.0.0.1:0", ServerCredentials.createInsecure(), (error, port) => {
      if (error !== null) {
        reject(error);
        return;
      }
      resolve(port);
    });
  });
}

class FailingBridgeClient {
  readonly options: CallOptions[] = [];

  constructor(private readonly code: number | undefined) {}

  mcpManifestChanged(
    _request: McpManifestChangedRequest,
    _metadata: Metadata,
    options: CallOptions,
    callback: (error: ServiceError | null, response: McpManifestChangedResponse) => void,
  ) {
    this.options.push(options);
    callback(Object.assign(new Error("bridge unavailable"), { code: this.code }) as ServiceError, { ack: undefined });
  }
}

class RepeatedUnknownThenReplayBridgeClient {
  readonly claimOptions: CallOptions[] = [];
  readonly commitOptions: CallOptions[] = [];
  readonly commitRequests: CommitMcpToolResultRequest[] = [];
  readonly commitAttachmentSnapshots: number[][] = [];
  readonly commitAuthorizations: unknown[] = [];

  claimMcpToolResult(
    _request: ClaimMcpToolResultRequest,
    _metadata: Metadata,
    options: CallOptions,
    callback: (error: ServiceError | null, response: ClaimMcpToolResultResponse) => void,
  ) {
    this.claimOptions.push(options);
    callback(null, {
      ack: { status: BridgeWriteStatus.BRIDGE_WRITE_STATUS_COMMITTED, runtimeInputId: "", runtimeWriteId: "", errorCode: "" },
      resultJson: "",
      declaration: undefined,
    });
  }

  commitMcpToolResult(
    request: CommitMcpToolResultRequest,
    metadata: Metadata,
    options: CallOptions,
    callback: (error: ServiceError | null, response: CommitMcpToolResultResponse) => void,
  ) {
    this.commitOptions.push(options);
    this.commitRequests.push(request);
    this.commitAttachmentSnapshots.push(Array.from(request.inlineMedia[0]?.data ?? []));
    this.commitAuthorizations.push(metadata.get("authorization"));
    if (this.commitRequests.length <= 4) {
      const codes = [
        status.DEADLINE_EXCEEDED,
        status.UNKNOWN,
        status.INTERNAL,
        status.RESOURCE_EXHAUSTED,
      ];
      callback(Object.assign(new Error("commit result unavailable"), { code: codes[this.commitRequests.length - 1] }) as ServiceError, {
        ack: undefined,
        refsOnlyResultJson: "",
        declaration: undefined,
      });
      return;
    }
    void metadata;
    callback(null, {
      ack: { status: BridgeWriteStatus.BRIDGE_WRITE_STATUS_COMMITTED, runtimeInputId: "", runtimeWriteId: "", errorCode: "" },
      refsOnlyResultJson: `{"response":{"status":1,"result_text":"ok","attachments":[]},"content_items":1,"refresh_triggered":false}`,
      materializationHandle: "sevt_tool_1",
      declaration: mcpMaterializationDeclaration(request, false),
    });
  }
}

class MaterializationBridgeClient {
  constructor(private readonly options: { readonly staleCustody?: boolean } = {}) {}

  claimMcpToolResult(
    _request: ClaimMcpToolResultRequest,
    _metadata: Metadata,
    _options: CallOptions,
    callback: (error: ServiceError | null, response: ClaimMcpToolResultResponse) => void,
  ) {
    callback(null, {
      ack: { status: BridgeWriteStatus.BRIDGE_WRITE_STATUS_COMMITTED, runtimeInputId: "", runtimeWriteId: "", errorCode: "" },
      resultJson: "",
      declaration: undefined,
    });
  }

  commitMcpToolResult(
    request: CommitMcpToolResultRequest,
    _metadata: Metadata,
    _options: CallOptions,
    callback: (error: ServiceError | null, response: CommitMcpToolResultResponse) => void,
  ) {
    callback(null, {
      ack: { status: BridgeWriteStatus.BRIDGE_WRITE_STATUS_COMMITTED, runtimeInputId: "", runtimeWriteId: "", errorCode: "" },
      refsOnlyResultJson: `{"response":{"status":1,"result_text":"ok","attachments":[]},"content_items":1,"refresh_triggered":false}`,
      materializationHandle: "sevt_tool_1",
      declaration: mcpMaterializationDeclaration(request, this.options.staleCustody === true),
    });
  }
}

class UnknownThenReplayBridgeClient extends MaterializationBridgeClient {
  readonly commitRequests: CommitMcpToolResultRequest[] = [];

  override commitMcpToolResult(
    request: CommitMcpToolResultRequest,
    metadata: Metadata,
    options: CallOptions,
    callback: (error: ServiceError | null, response: CommitMcpToolResultResponse) => void,
  ) {
    this.commitRequests.push(request);
    if (this.commitRequests.length === 1) {
      callback(Object.assign(new Error("connection closed after commit"), { code: status.UNAVAILABLE }) as ServiceError, {
        ack: undefined,
        refsOnlyResultJson: "",
        declaration: undefined,
      });
      return;
    }
    super.commitMcpToolResult(request, metadata, options, callback);
  }
}

class StaleClaimReplayBridgeClient extends MaterializationBridgeClient {
  override claimMcpToolResult(
    request: ClaimMcpToolResultRequest,
    _metadata: Metadata,
    _options: CallOptions,
    callback: (error: ServiceError | null, response: ClaimMcpToolResultResponse) => void,
  ) {
    callback(null, {
      ack: { status: BridgeWriteStatus.BRIDGE_WRITE_STATUS_DUPLICATE, runtimeInputId: "", runtimeWriteId: "", errorCode: "" },
      resultJson: `{"response":{"status":1,"result_text":"ok","attachments":[]},"content_items":1,"refresh_triggered":false}`,
      materializationHandle: request.toolUseEventId,
      declaration: {
        receipts: [{
          sessionThreadId: request.scope?.sessionThreadId ?? "",
          operationKind: "commit_mcp_tool_result",
          sourceKind: "mcp_tool_execution",
          sourceId: stableMcpMaterializationSourceId(request),
          events: [],
          messages: [],
          pendingAttachmentDeltaJson: [],
          pendingToolDeltaJson: [],
          prefixConsumptions: [],
          declarationDigest: "digest_from_durable_operation",
          requestReschedule: undefined,
          childLifecycle: [],
          requestStart: undefined,
          idleCloseout: undefined,
          compactedThroughMessageSequence: undefined,
        }],
        observedBindingId: "bind_new",
        observedBindingGeneration: 2,
        applicationDisposition: ReceiptApplicationDisposition.RECEIPT_APPLICATION_DISPOSITION_STALE_CUSTODY,
      },
    });
  }
}

class MalformedAttachmentReceiptBridgeClient extends MaterializationBridgeClient {
  override commitMcpToolResult(
    request: CommitMcpToolResultRequest,
    _metadata: Metadata,
    _options: CallOptions,
    callback: (error: ServiceError | null, response: CommitMcpToolResultResponse) => void,
  ) {
    const raw = JSON.parse(request.resultJson) as {
      response: {
        attachments: Array<{
          mime: string;
          size_bytes: number;
          suggested_filename: string;
          attachment_ref?: string;
        }>;
      };
    };
    const attachment = raw.response.attachments[0]!;
    attachment.attachment_ref = "att_1";
    const refsOnlyResultJson = JSON.stringify(raw);
    const declaration = mcpMaterializationDeclaration(request, false);
    declaration.receipts[0]!.pendingAttachmentDeltaJson = [JSON.stringify({
      origin: {
        transient: {
          attachmentRef: "att_wrong",
          sourceToolUseEventId: request.toolUseEventId,
          sourcePath: `mcp:${request.mcpServerName}/${attachment.suggested_filename}`,
          detail: "auto",
        },
      },
      mime: attachment.mime,
      filename: attachment.suggested_filename,
    })];
    callback(null, {
      ack: { status: BridgeWriteStatus.BRIDGE_WRITE_STATUS_COMMITTED, runtimeInputId: "", runtimeWriteId: "", errorCode: "" },
      refsOnlyResultJson,
      materializationHandle: request.toolUseEventId,
      declaration,
    });
  }
}

function mcpMaterializationDeclaration(
  request: CommitMcpToolResultRequest,
  staleCustody: boolean,
): DeclarationResponse {
  return {
    receipts: [{
      sessionThreadId: request.scope?.sessionThreadId ?? "",
      operationKind: "commit_mcp_tool_result",
      sourceKind: "mcp_tool_execution",
      sourceId: stableMcpMaterializationSourceId(request),
      events: [],
      messages: [],
      pendingAttachmentDeltaJson: [],
      pendingToolDeltaJson: [],
      prefixConsumptions: [],
      declarationDigest: mcpMaterializationDeclarationDigest(request),
      requestReschedule: undefined,
      childLifecycle: [],
      requestStart: undefined,
      idleCloseout: undefined,
      compactedThroughMessageSequence: undefined,
    }],
    observedBindingId: request.scope?.binding?.bindingId ?? "",
    observedBindingGeneration: request.scope?.binding?.bindingGeneration ?? 0,
    applicationDisposition: staleCustody
      ? ReceiptApplicationDisposition.RECEIPT_APPLICATION_DISPOSITION_STALE_CUSTODY
      : ReceiptApplicationDisposition.RECEIPT_APPLICATION_DISPOSITION_CURRENT_CUSTODY,
  };
}

function stableMcpMaterializationSourceId(
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

function mcpMaterializationDeclarationDigest(request: CommitMcpToolResultRequest): string {
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

function mcpIdempotencyContext() {
  return {
    requestId: "req_1",
    workspaceId: "wksp_1",
    sessionId: "sesn_1",
    sessionThreadId: "thrd_1",
    bindingId: "bind_1",
    bindingGeneration: 1,
    runtimePodUid: "pod_1",
    mcpServerName: "github",
    toolName: "create_issue",
    inputJson: "{}",
  };
}
