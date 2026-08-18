import { describe, expect, test } from "bun:test";
import { Metadata, Server, ServerCredentials, status } from "@grpc/grpc-js";
import type { CallOptions, ServiceError } from "@grpc/grpc-js";
import {
  AgentRuntimeBridgeServiceService,
} from "@tetral/gateway-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type {
  AgentRuntimeBridgeServiceServer,
  ClaimMcpToolResultRequest,
  ClaimMcpToolResultResponse,
  CommitMcpToolResultRequest,
  CommitMcpToolResultResponse,
  McpManifestChangedRequest,
  McpManifestChangedResponse,
  RelinquishMcpToolResultRequest,
  RelinquishMcpToolResultResponse,
} from "@tetral/gateway-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import {
  McpErrorKind,
  McpRetryStatus,
  RunMcpToolStatus,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import {
  BridgeAPIManifestChangeNotifier,
  BridgeAPIMcpToolResultIdempotencyStore,
  BridgeMcpCommitGrpcMessageBytes,
  MCP_CLAIM_RPC_TIMEOUT_MS,
  MCP_COMMIT_RPC_TIMEOUT_MS,
  MCP_MANIFEST_CHANGE_RPC_TIMEOUT_MS,
  bridgeMcpCommitGrpcChannelOptions,
} from "../../src/bridge-client.js";
import type {
  BridgeManifestChangedClient,
  BridgeMcpToolResultClient,
} from "../../src/bridge-client.js";
import {
  InMemoryMcpIdempotencyStore,
  McpIdempotencyStaleCustodyError,
} from "../../src/idempotency.js";
import type {
  McpIdempotencyContext,
  PendingStoredRunMcpToolResponse,
} from "../../src/idempotency.js";
import { McpGrpcKeepaliveTimeMs, McpGrpcKeepaliveTimeoutMs } from "../../src/transport.js";

const key = { toolUseEventId: "sevt_tool_1" };
const executor = {
  mcpServerName: "github",
  toolName: "create_issue",
  inputJson: '{"title":"Hello"}',
};

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

  test("sends manifest notifications with outbound metadata and maps the closed result", async () => {
    const client = new RecordingManifestClient({ committed: {} });
    const notifier = manifestNotifier(client, async () => {
      const metadata = new Metadata();
      metadata.set("authorization", "bearer projected-token");
      return metadata;
    });

    await expect(notifier.notify(manifestRequest())).resolves.toEqual({ ok: true, duplicate: false });
    expect(client.calls).toEqual([{
      request: manifestRequest(),
      authorization: ["bearer projected-token"],
    }]);

    await expect(manifestNotifier(new RecordingManifestClient({ duplicate: {} })).notify(manifestRequest()))
      .resolves.toEqual({ ok: true, duplicate: true });
  });

  test("classifies token and transport failures", async () => {
    const tokenFailure = manifestNotifier(
      new RecordingManifestClient({ committed: {} }),
      async () => { throw new Error("unavailable"); },
    );
    await expect(tokenFailure.notify(manifestRequest())).resolves.toMatchObject({
      ok: false,
      retryable: true,
      code: "bridge_token_unavailable",
    });

    for (const testCase of [
      { code: undefined, retryable: true, resultCode: "bridge_unavailable" },
      { code: status.UNAVAILABLE, retryable: true, resultCode: `grpc_${status.UNAVAILABLE}` },
      { code: status.DEADLINE_EXCEEDED, retryable: true, resultCode: `grpc_${status.DEADLINE_EXCEEDED}` },
      { code: status.INVALID_ARGUMENT, retryable: false, resultCode: `grpc_${status.INVALID_ARGUMENT}` },
    ] as const) {
      await expect(manifestNotifier(new FailingManifestClient(testCase.code)).notify(manifestRequest()))
        .resolves.toMatchObject({
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
      await expect(notifier.notify(manifestRequest())).resolves.toMatchObject({
        ok: false,
        retryable: true,
        code: `grpc_${status.DEADLINE_EXCEEDED}`,
      });
      expect(MCP_MANIFEST_CHANGE_RPC_TIMEOUT_MS).toBe(5_000);
    } finally {
      server.forceShutdown();
    }
  });

  test("does not infer durable manifest settlement from ResourceExhausted details", async () => {
    await expect(manifestNotifier(new FailingManifestClient(
      status.RESOURCE_EXHAUSTED,
      "mcp manifest tools exceed the accepted byte limit",
    )).notify(manifestRequest())).resolves.toMatchObject({
      ok: false,
      retryable: false,
      code: `grpc_${status.RESOURCE_EXHAUSTED}`,
    });
  });

  test("rejects zero-arm and multi-arm manifest results", async () => {
    for (const response of [{}, { committed: {}, duplicate: {} }]) {
      await expect(manifestNotifier(new RecordingManifestClient(response)).notify(manifestRequest()))
        .resolves.toEqual({
          ok: false,
          retryable: false,
          code: "bridge_manifest_change_rejected",
          message: "mcp manifest change notification rejected",
        });
    }
  });
});

describe("BridgeAPIMcpToolResultIdempotencyStore", () => {
  test("keeps the same tool-use event isolated across thread-scoped local caches", () => {
    const store = new InMemoryMcpIdempotencyStore(executor);
    const main = mcpContext();
    const child = { ...main, sessionThreadId: "thrd_child", claimId: "mcpclaim_child" };

    expect(store.claim(key, main)).toEqual({ status: "new", executor });
    expect(store.claim(key, child)).toEqual({ status: "new", executor });
    store.store(key, pendingResult("main"), main);
    store.store(key, pendingResult("child"), child);

    expect(store.claim(key, main)).toMatchObject({ status: "replay", stored: { response: { resultText: "main" } } });
    expect(store.claim(key, child)).toMatchObject({ status: "replay", stored: { response: { resultText: "child" } } });
  });

  test("claims only target and custody while Bridge supplies executor facts", async () => {
    const client = new ScriptedMcpClient([{ response: { acquired: executor } }]);
    const store = durableStore(client);

    await expect(store.claim(key, mcpContext())).resolves.toEqual({ status: "new", executor });
    expect(client.claimRequests).toEqual([claimRequest()]);
  });

  test("keys local execution ownership by the exact active claim attempt", async () => {
    const client = new ScriptedMcpClient([
      { response: { acquired: executor } },
      { response: { acquired: executor } },
      { response: { acquired: executor } },
    ]);
    const store = durableStore(client);
    const first = mcpContext();
    const takeover = { ...first, claimId: "mcpclaim_takeover" };

    await expect(store.claim(key, first)).resolves.toEqual({ status: "new", executor });
    await expect(store.claim(key, first)).resolves.toEqual({ status: "in_flight" });
    await expect(store.claim(key, takeover)).resolves.toEqual({ status: "new", executor });
  });

  test("durably relinquishes the exact active claim and replays a lost acknowledgement", async () => {
    const client = new ScriptedMcpClient(
      [{ response: { acquired: executor } }],
      [],
      [
        { error: serviceError(status.UNAVAILABLE) },
        { response: { duplicate: {} } },
      ],
    );
    const delays: number[] = [];
    const store = durableStore(client, async (delayMs) => { delays.push(delayMs); });

    await store.claim(key, mcpContext());
    await expect(store.fail(key, mcpContext())).resolves.toBeUndefined();

    expect(delays).toEqual([100]);
    expect(client.relinquishRequests).toEqual([
      relinquishRequest(),
      relinquishRequest(),
    ]);
  });

  test("requires exactly one typed MCP relinquish result", async () => {
    for (const response of [{}, { relinquished: {}, stale: {} }]) {
      const store = durableStore(new ScriptedMcpClient([], [], [{ response }]));
      await expect(store.fail(key, mcpContext()))
        .rejects.toThrow("RelinquishMcpToolResult returned an invalid result variant");
    }
    await expect(durableStore(new ScriptedMcpClient([], [], [{ response: { stale: {} } }])).fail(key, mcpContext()))
      .resolves.toBeUndefined();
  });

  test("reads already-completed direct durable facts without a receipt", async () => {
    const client = new ScriptedMcpClient([{
      response: { alreadyCompleted: { resultJson: storedResultJSON("durable") } },
    }]);
    await expect(durableStore(client).claim(key, mcpContext())).resolves.toMatchObject({
      status: "replay",
      stored: { response: { resultText: "durable" } },
    });
  });

  test("maps in-flight, stale, and conflict claim results", async () => {
    await expect(durableStore(new ScriptedMcpClient([{ response: { inFlight: {} } }])).claim(key, mcpContext()))
      .resolves.toEqual({ status: "in_flight" });
    await expect(durableStore(new ScriptedMcpClient([{ response: { stale: {} } }])).claim(key, mcpContext()))
      .resolves.toEqual({ status: "stale_custody" });
    await expect(durableStore(new ScriptedMcpClient([{ error: serviceError(status.ALREADY_EXISTS) }])).claim(key, mcpContext()))
      .resolves.toEqual({ status: "conflict" });
  });

  test("rejects zero-arm and multi-arm claim results", async () => {
    for (const response of [{}, { acquired: executor, inFlight: {} }]) {
      await expect(durableStore(new ScriptedMcpClient([{ response }])).claim(key, mcpContext()))
        .rejects.toThrow("ClaimMcpToolResult returned an invalid result variant");
    }
  });

  test("requires authenticated claim context", async () => {
    const store = durableStore(new ScriptedMcpClient([]));
    await expect(store.claim(key)).rejects.toThrow("mcp tool idempotency context is required");
    await expect(store.store(key, pendingResult("ok"))).rejects.toThrow("mcp tool idempotency context is required");
  });

  test("bounds a hung Claim RPC before external execution begins", async () => {
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
      await expect(store.claim(key, mcpContext())).rejects.toMatchObject({ code: status.DEADLINE_EXCEEDED });
      expect(MCP_CLAIM_RPC_TIMEOUT_MS).toBe(10_000);
    } finally {
      server.forceShutdown();
    }
  });

  test("converges a lost commit ACK from its receipt without a fallible post-commit Claim", async () => {
    const client = new ScriptedMcpClient([
      { response: { acquired: executor } },
      { error: serviceError(status.UNAVAILABLE) },
    ], [
      { error: serviceError(status.UNKNOWN) },
      { response: { committed: { attachmentRef: "" } } },
    ]);
    const delays: number[] = [];
    const store = durableStore(client, async (delayMs) => { delays.push(delayMs); });

    await store.claim(key, mcpContext());
    await expect(store.store(key, pendingResult("ok"), mcpContext())).resolves.toMatchObject({ resultText: "ok" });

    expect(delays).toEqual([100]);
    expect(client.commitRequests).toHaveLength(2);
    expect(client.commitRequests[0]).toBe(client.commitRequests[1]);
    expect(client.commitRequests[0]).toEqual(commitRequest(storedResultJSON("ok")));
    expect(client.claimRequests).toHaveLength(1);
  });

  test("materializes one terminal failure after Bridge rejects provider result bytes", async () => {
    const terminal = storedTerminalFailureJSON();
    const client = new ScriptedMcpClient([
      { response: { acquired: executor } },
    ], [
      { error: serviceError(status.INVALID_ARGUMENT) },
      { response: { committed: { attachmentRef: "" } } },
    ]);
    const store = durableStore(client, async () => {
      throw new Error("deterministic rejection must not back off");
    });

    await store.claim(key, mcpContext());
    await expect(store.store(key, pendingResult("provider output"), mcpContext())).resolves.toMatchObject({
      status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_RUNTIME_ERROR,
      resultText: "MCP tool result could not be committed.",
      errorKind: McpErrorKind.MCP_ERROR_KIND_COMMIT_FAILED,
      retryStatus: McpRetryStatus.MCP_RETRY_STATUS_TERMINAL,
    });
    expect(client.commitRequests).toHaveLength(2);
    expect(client.commitRequests[1]).not.toBe(client.commitRequests[0]);
    expect(client.commitRequests[1]?.inlineMedia).toEqual([]);
    expect(client.commitRequests[1]?.resultJson).toBe(terminal);
  });

  test("retries the same canonical failure bytes after an unknown outcome", async () => {
    const terminal = storedTerminalFailureJSON();
    const client = new ScriptedMcpClient([
      { response: { acquired: executor } },
    ], [
      { error: serviceError(status.INVALID_ARGUMENT) },
      { error: serviceError(status.UNKNOWN) },
      { response: { committed: { attachmentRef: "" } } },
    ]);
    const delays: number[] = [];
    const store = durableStore(client, async (delayMs) => { delays.push(delayMs); });

    await store.claim(key, mcpContext());
    await store.store(key, pendingResult("provider output"), mcpContext());

    expect(delays).toEqual([300]);
    expect(client.commitRequests).toHaveLength(3);
    expect(client.commitRequests[1]).not.toBe(client.commitRequests[0]);
    expect(client.commitRequests[2]).toBe(client.commitRequests[1]);
  });

  test("relinquishes custody without fabricating a replacement on a stale commit", async () => {
    const client = new ScriptedMcpClient([
      { response: { acquired: executor } },
    ], [{ error: serviceError(status.FAILED_PRECONDITION) }]);
    const store = durableStore(client, async () => {
      throw new Error("stale custody must not back off");
    });

    await store.claim(key, mcpContext());
    await expect(store.store(key, pendingResult("must not be replaced"), mcpContext()))
      .rejects.toBeInstanceOf(McpIdempotencyStaleCustodyError);
    expect(client.commitRequests).toHaveLength(1);
  });

  test("maps a typed stale commit and replays zero-arm or multi-arm commit ACK uncertainty", async () => {
    const staleStore = durableStore(new ScriptedMcpClient(
      [{ response: { acquired: executor } }],
      [{ response: { stale: {} } }],
    ));
    await staleStore.claim(key, mcpContext());
    await expect(staleStore.store(key, pendingResult("stale"), mcpContext()))
      .rejects.toBeInstanceOf(McpIdempotencyStaleCustodyError);

    for (const response of [{}, {
      committed: { attachmentRef: "att_invalid_committed" },
      duplicate: { attachmentRef: "att_invalid_duplicate" },
    }]) {
      const store = durableStore(new ScriptedMcpClient(
        [
          { response: { acquired: executor } },
        ],
        [{ response }, { response: { duplicate: { attachmentRef: "" } } }],
      ));
      await store.claim(key, mcpContext());
      await expect(store.store(key, pendingResult("converged"), mcpContext()))
        .resolves.toMatchObject({ resultText: "converged" });
    }
  });

  test("keeps unknown commit bytes frozen through capped backoff and refreshed metadata", async () => {
    const attachmentBytes = Uint8Array.from([1, 2, 3]);
    const client = new ScriptedMcpClient([
      { response: { acquired: executor } },
    ], [
      { error: serviceError(status.DEADLINE_EXCEEDED) },
      { error: serviceError(status.UNKNOWN) },
      { error: serviceError(status.INTERNAL) },
      { error: serviceError(status.RESOURCE_EXHAUSTED) },
      { response: { duplicate: { attachmentRef: "att_bridge_1" } } },
    ]);
    const delays: number[] = [];
    let metadataGeneration = 0;
    const store = new BridgeAPIMcpToolResultIdempotencyStore({
      address: "unused",
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
        delays.push(delayMs);
        attachmentBytes[0] = 9;
      },
    });

    await store.claim(key, mcpContext());
    await expect(store.store(key, {
      ...pendingResult("ok"),
      response: {
        ...pendingResult("ok").response,
        attachments: [{ data: attachmentBytes, mime: "image/png", suggestedFilename: "plot.png" }],
      },
    }, mcpContext())).resolves.toMatchObject({ resultText: "ok" });

    expect(client.claimOptions).toEqual([
      { deadline: new Date(1_321) },
    ]);
    expect(client.commitOptions).toEqual(Array.from({ length: 5 }, () => ({ deadline: new Date(2_234) })));
    expect(new Set(client.commitRequests).size).toBe(1);
    expect(client.commitAttachmentSnapshots).toEqual(Array.from({ length: 5 }, () => [1, 2, 3]));
    expect(client.commitAuthorizations).toEqual([
      ["bearer token_2"],
      ["bearer token_3"],
      ["bearer token_4"],
      ["bearer token_5"],
      ["bearer token_6"],
    ]);
    expect(delays).toEqual([100, 300, 1_000, 1_000]);
    expect(MCP_COMMIT_RPC_TIMEOUT_MS).toBe(10_000);
  });
});

type Scripted<T> = { readonly response: T } | { readonly error: ServiceError };

class RecordingManifestClient implements BridgeManifestChangedClient {
  readonly calls: Array<{ readonly request: McpManifestChangedRequest; readonly authorization: unknown }> = [];

  constructor(private readonly response: McpManifestChangedResponse) {}

  mcpManifestChanged(
    request: McpManifestChangedRequest,
    metadata: Metadata,
    _options: CallOptions,
    callback: (error: ServiceError | null, response: McpManifestChangedResponse) => void,
  ): unknown {
    this.calls.push({ request, authorization: metadata.get("authorization") });
    callback(null, this.response);
    return undefined;
  }
}

class FailingManifestClient implements BridgeManifestChangedClient {
  constructor(private readonly code: number | undefined, private readonly details?: string) {}

  mcpManifestChanged(
    _request: McpManifestChangedRequest,
    _metadata: Metadata,
    _options: CallOptions,
    callback: (error: ServiceError | null, response: McpManifestChangedResponse) => void,
  ): unknown {
    callback(serviceError(this.code, this.details), {});
    return undefined;
  }
}

class ScriptedMcpClient implements BridgeMcpToolResultClient {
  readonly claimRequests: ClaimMcpToolResultRequest[] = [];
  readonly claimOptions: CallOptions[] = [];
  readonly commitRequests: CommitMcpToolResultRequest[] = [];
  readonly commitOptions: CallOptions[] = [];
  readonly commitAttachmentSnapshots: number[][] = [];
  readonly commitAuthorizations: unknown[] = [];
  readonly relinquishRequests: RelinquishMcpToolResultRequest[] = [];
  readonly relinquishOptions: CallOptions[] = [];

  constructor(
    private readonly claims: Scripted<ClaimMcpToolResultResponse>[],
    private readonly commits: Scripted<CommitMcpToolResultResponse>[] = [],
    private readonly relinquishes: Scripted<RelinquishMcpToolResultResponse>[] = [],
  ) {}

  claimMcpToolResult(
    request: ClaimMcpToolResultRequest,
    _metadata: Metadata,
    options: CallOptions,
    callback: (error: ServiceError | null, response: ClaimMcpToolResultResponse) => void,
  ): unknown {
    this.claimRequests.push(request);
    this.claimOptions.push(options);
    const scripted = this.claims.shift();
    if (scripted === undefined) throw new Error("missing scripted claim response");
    if ("error" in scripted) callback(scripted.error, {});
    else callback(null, scripted.response);
    return undefined;
  }

  commitMcpToolResult(
    request: CommitMcpToolResultRequest,
    metadata: Metadata,
    options: CallOptions,
    callback: (error: ServiceError | null, response: CommitMcpToolResultResponse) => void,
  ): unknown {
    this.commitRequests.push(request);
    this.commitOptions.push(options);
    this.commitAttachmentSnapshots.push(Array.from(request.inlineMedia[0]?.data ?? []));
    this.commitAuthorizations.push(metadata.get("authorization"));
    const scripted = this.commits.shift();
    if (scripted === undefined) throw new Error("missing scripted commit response");
    if ("error" in scripted) callback(scripted.error, {});
    else callback(null, scripted.response);
    return undefined;
  }

  relinquishMcpToolResult(
    request: RelinquishMcpToolResultRequest,
    _metadata: Metadata,
    options: CallOptions,
    callback: (error: ServiceError | null, response: RelinquishMcpToolResultResponse) => void,
  ): unknown {
    this.relinquishRequests.push(request);
    this.relinquishOptions.push(options);
    const scripted = this.relinquishes.shift() ?? { response: { relinquished: {} } };
    if ("error" in scripted) callback(scripted.error, {});
    else callback(null, scripted.response);
    return undefined;
  }
}

function manifestNotifier(
  client: BridgeManifestChangedClient,
  metadataFactory: () => Promise<Metadata> = async () => new Metadata(),
): BridgeAPIManifestChangeNotifier {
  return new BridgeAPIManifestChangeNotifier({
    address: "unused",
    tokenPath: "/token",
    client,
    metadataFactory,
  });
}

function durableStore(
  client: BridgeMcpToolResultClient,
  sleep: (delayMs: number) => Promise<void> = async () => undefined,
): BridgeAPIMcpToolResultIdempotencyStore {
  return new BridgeAPIMcpToolResultIdempotencyStore({
    address: "unused",
    tokenPath: "/token",
    client,
    metadataFactory: async () => new Metadata(),
    sleep,
  });
}

function manifestRequest(): McpManifestChangedRequest {
  return {
    workspaceId: "wksp_1",
    sessionId: "sesn_1",
    mcpServerName: "github",
    manifestEtag: "etag_1",
  };
}

function mcpContext(): McpIdempotencyContext {
  return {
    claimId: "mcpclaim_1",
    workspaceId: "wksp_1",
    sessionId: "sesn_1",
    sessionThreadId: "thrd_1",
    bindingId: "bind_1",
    bindingGeneration: 1,
    runtimePodUid: "pod_1",
  };
}

function relinquishRequest(): RelinquishMcpToolResultRequest {
  const context = mcpContext();
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

function claimRequest(): ClaimMcpToolResultRequest {
  const context = mcpContext();
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

function commitRequest(resultJson: string): CommitMcpToolResultRequest {
  return {
    scope: claimRequest().scope,
    toolUseEventId: key.toolUseEventId,
    claimId: mcpContext().claimId,
    resultJson,
    inlineMedia: [],
  };
}

function pendingResult(resultText: string): PendingStoredRunMcpToolResponse {
  return {
    response: {
      status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED,
      resultText,
      attachments: [],
    },
    contentItems: 1,
    refreshTriggered: false,
  };
}

function storedResultJSON(resultText: string): string {
  return JSON.stringify({
    response: {
      status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED,
      result_text: resultText,
      attachments: [],
      error_kind: null,
      retry_status: null,
    },
    content_items: 1,
    refresh_triggered: false,
  });
}

function storedTerminalFailureJSON(): string {
  return JSON.stringify({
    response: {
      status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_RUNTIME_ERROR,
      result_text: "MCP tool result could not be committed.",
      attachments: [],
      error_kind: McpErrorKind.MCP_ERROR_KIND_COMMIT_FAILED,
      retry_status: McpRetryStatus.MCP_RETRY_STATUS_TERMINAL,
    },
    content_items: 0,
    refresh_triggered: false,
  });
}

function serviceError(code: number | undefined, details?: string): ServiceError {
  return Object.assign(new Error("bridge unavailable"), { code, details }) as ServiceError;
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
