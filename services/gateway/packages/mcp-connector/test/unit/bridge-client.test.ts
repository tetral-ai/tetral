import { describe, expect, test } from "bun:test";
import { Metadata, Server, ServerCredentials, status } from "@grpc/grpc-js";
import type { ServiceError } from "@grpc/grpc-js";
import {
  AgentRuntimeBridgeServiceService,
  BridgeWriteStatus,
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
  McpManifestChangedRequest,
  McpManifestChangedResponse,
} from "@tetral/gateway-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type { CallOptions } from "@grpc/grpc-js";
import { RunMcpToolStatus } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";

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

  test("sets a bounded commit deadline and propagates deadline expiry for classification", async () => {
    const client = new CommitDeadlineBridgeClient();
    const store = new BridgeAPIMcpToolResultIdempotencyStore({
      address: "bridge.tetral-system.svc.cluster.local:9090",
      tokenPath: "/token",
      client,
      metadataFactory: async () => new Metadata(),
      claimTimeoutMs: 321,
      commitTimeoutMs: 1_234,
      now: () => 1_000,
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
        attachments: [],
      },
      contentItems: 1,
      refreshTriggered: false,
    }, context)).rejects.toMatchObject({ code: status.DEADLINE_EXCEEDED });

    expect(client.claimOptions).toEqual([{ deadline: new Date(1_321) }]);
    expect(client.commitOptions).toEqual([{ deadline: new Date(2_234) }]);
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

class CommitDeadlineBridgeClient {
  readonly claimOptions: CallOptions[] = [];
  readonly commitOptions: CallOptions[] = [];

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
    });
  }

  commitMcpToolResult(
    _request: CommitMcpToolResultRequest,
    _metadata: Metadata,
    options: CallOptions,
    callback: (error: ServiceError | null, response: CommitMcpToolResultResponse) => void,
  ) {
    this.commitOptions.push(options);
    callback(Object.assign(new Error("deadline exceeded"), { code: status.DEADLINE_EXCEEDED }) as ServiceError, {
      ack: undefined,
      refsOnlyResultJson: "",
    });
  }
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
