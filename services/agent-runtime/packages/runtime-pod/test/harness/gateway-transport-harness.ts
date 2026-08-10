import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { Metadata, Server, ServerCredentials } from "@grpc/grpc-js";
import type { CallOptions, ClientUnaryCall } from "@grpc/grpc-js";
import {
  AgentRuntimeBridgeServiceClient,
  BridgeWriteStatus,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type {
  AcceptSandboxExecutionRequest,
  AwaitSandboxExecutionRequest,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import {
  ProviderFinishReason,
  ProviderGatewayServiceService,
  ProviderRequest as ProviderRequestMessage,
  ProviderRequestKind,
  ProviderStreamEventType,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type {
  ProviderGatewayServiceServer,
  ProviderRequest,
  ProviderStreamEvent,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { Effect, Stream } from "effect";
import { RuntimeMessageSchema } from "../../../core/src/contracts/runtime.js";
import type { RuntimeMessage, RuntimePart } from "../../../core/src/contracts/runtime.js";
import { toGatewayRuntimeMessages } from "../../../core/src/runtime/message-projection.js";
import { estimatedRuntimeMessagesTokens } from "../../../core/src/thread-loop/compaction.js";
import { assembleProviderCallRequest } from "../../../core/src/thread-loop/provider-request.js";
import { createToolCatalog, lookupToolEntry } from "../../../core/src/tools/tool-catalog.js";
import type { ToolCatalog } from "../../../core/src/tools/tool-catalog.js";
import type { RuntimeToolExecutionRequest } from "../../../core/src/thread-loop/tool-execution.js";
import { createGatewayGrpcServer } from "../../../../../gateway/packages/provider-gateway/src/grpc-server.js";
import { ProviderGatewayServiceShell } from "../../../../../gateway/packages/provider-gateway/src/service.js";
import { ProviderClientRegistry } from "../../../../../gateway/packages/provider-gateway/src/providers/clients.js";
import type { ProviderCredentialResolver } from "../../../../../gateway/packages/provider-gateway/src/providers/credentials.js";
import { MaxGatewayRequestGrpcMessageBytes, gatewayGrpcChannelOptions } from "../../src/bounds.js";
import { RuntimePodGatewayClient } from "../../src/gateway-client.js";
import { RuntimePodToolRunner } from "../../src/tool-runner.js";
import { BridgeAPIContextLoader } from "../../src/bridge-client.js";
import { createLLMService } from "../../../core/src/llm/llm-service.js";
import type { GatewayClientError } from "../../../core/src/llm/llm-service.js";

const CapacityTargetTokens = 1_050_000;
const RequiredHeadroomRatio = 0.20;
const createdAt = "2026-08-05T00:00:00.000Z";
const SystemWireMarkers = [
  "TETRAL_CAPACITY_BASE",
  "TETRAL_CAPACITY_AGENT",
  "TETRAL_CAPACITY_MEMORY",
  "TETRAL_CAPACITY_SKILL",
] as const;

export interface GatewayCapacityMeasurement {
  readonly vector: string;
  readonly estimatedTokens: number;
  readonly modelLimitTokens: number;
  readonly encodedRequestBytes: number;
  readonly receivedRequestBytes: number;
  readonly configuredFuseBytes: number;
  readonly headroomRatio: number;
  readonly loweredBytesByFamily: Readonly<Record<string, number>>;
  readonly loadedContextBytes?: number;
}

const CapacityRoutes = [
  { family: "anthropic", providerId: "anthropic", modelId: "claude-opus-4-8", supplyMode: "anthropic-api-key" },
  { family: "openai", providerId: "openai", modelId: "gpt-5.5", supplyMode: "openai-api-key" },
  { family: "openai-compatible", providerId: "deepseek", modelId: "deepseek-v4-pro", supplyMode: "deepseek-api-key" },
] as const;

type CapacityRoute = (typeof CapacityRoutes)[number];

/** Runs the catalog-capacity proof through production projection, assembly, transport, and lowering. */
export async function runGatewayCapacityProof(): Promise<readonly GatewayCapacityMeasurement[]> {
  const vectors = await capacityVectors();
  const receivedBytes = new Map<string, number>();
  const wireBytes = new Map<string, number>();
  let activeCapture: string | undefined;
  let activeMarkers: readonly string[] = [];
  const providerRegistry = new ProviderClientRegistry({
    fetch: Object.assign(async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = new Request(input, init);
      if (activeCapture === undefined) {
        throw new Error("capacity proof fetch has no active request");
      }
      const body = await request.clone().text();
      wireBytes.set(activeCapture, Buffer.byteLength(body, "utf8"));
      for (const marker of activeMarkers) {
        if (!body.includes(marker)) {
          throw new Error(`capacity proof provider wire dropped marker ${marker}`);
        }
      }
      return new Response(JSON.stringify({ error: { type: "server_error", message: "synthetic capacity response" } }), {
        status: 500,
        headers: { "content-type": "application/json" },
      });
    }, { preconnect: () => undefined }),
  });
  const service = new ProviderGatewayServiceShell({
    authenticator: {
      authenticate: async () => ({
        ok: true,
        serviceAccount: { namespace: "tetral", name: "agent-runtime", podUid: "pod_capacity" },
      }),
    },
    runtimeBindingTokenVerifier: { verify: () => true },
    ready: () => true,
    logger: { info: () => undefined, error: () => undefined },
    providerStreamer: {
      stream: async function* (input) {
        receivedBytes.set(input.request.requestId, ProviderRequestMessage.encode(input.request).finish().byteLength);
        yield* providerRegistry.stream(input);
      },
    },
    credentialResolver: capacityCredentialResolver(),
  });
  const server = createGatewayGrpcServer(service);
  const port = await server.bind("127.0.0.1:0");
  const client = new RuntimePodGatewayClient({
    address: `127.0.0.1:${port}`,
    tokenPath: "/unused",
    channelOptions: gatewayGrpcChannelOptions(),
    metadataFactory: async () => new Metadata(),
  });

  try {
    const measurements: GatewayCapacityMeasurement[] = [];
    for (const vector of vectors) {
      const loweredBytesByFamily: Record<string, number> = {};
      let encodedRequestBytes = 0;
      let receivedRequestBytes = 0;
      for (const route of CapacityRoutes) {
        const request = requestForRoute(vector.request, route);
        const captureKey = `${vector.name}:${route.family}`;
        activeCapture = captureKey;
        activeMarkers = vector.wireMarkers;
          const events = await collectGatewayEvents(client, request);
        if (events.length === 0) {
          throw new Error("capacity proof transport returned no terminal provider event");
        }
        const received = receivedBytes.get(request.requestId);
        const wire = wireBytes.get(captureKey);
        if (received === undefined || wire === undefined) {
          throw new Error("capacity proof did not cross the production Gateway and provider wire");
        }
        encodedRequestBytes = Math.max(encodedRequestBytes, ProviderRequestMessage.encode(request).finish().byteLength);
        receivedRequestBytes = Math.max(receivedRequestBytes, received);
        loweredBytesByFamily[route.family] = wire;
      }
      measurements.push({
        vector: vector.name,
        estimatedTokens: vector.estimatedTokens,
        modelLimitTokens: CapacityTargetTokens,
        encodedRequestBytes,
        receivedRequestBytes,
        configuredFuseBytes: MaxGatewayRequestGrpcMessageBytes,
        headroomRatio: (MaxGatewayRequestGrpcMessageBytes - encodedRequestBytes) / MaxGatewayRequestGrpcMessageBytes,
        loweredBytesByFamily,
        ...(vector.loadedContextBytes === undefined ? {} : { loadedContextBytes: vector.loadedContextBytes }),
      });
    }
    return measurements;
  } finally {
    await server.shutdown();
  }
}

/** Proves a smaller Runtime request carrier rejects a production-path maximum vector. */
export async function runGatewayCapacityFuseMutation(): Promise<void> {
  const vector = (await capacityVectors())[0];
  if (vector === undefined) {
    throw new Error("capacity proof vector is absent");
  }
  const server = createGatewayGrpcServer(new ProviderGatewayServiceShell({
    authenticator: {
      authenticate: async () => ({
        ok: true,
        serviceAccount: { namespace: "tetral", name: "agent-runtime", podUid: "pod_capacity" },
      }),
    },
    runtimeBindingTokenVerifier: { verify: () => true },
    ready: () => true,
    logger: { info: () => undefined, error: () => undefined },
    providerStreamer: { stream: (input) => successfulCapacityStream(input.request) },
    credentialResolver: capacityCredentialResolver(),
  }));
  const port = await server.bind("127.0.0.1:0");
  const client = new RuntimePodGatewayClient({
    address: `127.0.0.1:${port}`,
    tokenPath: "/unused",
    channelOptions: { ...gatewayGrpcChannelOptions(), "grpc.max_send_message_length": 4 * 1024 * 1024 },
    metadataFactory: async () => new Metadata(),
  });
  try {
      await collectGatewayEvents(client, requestForRoute(vector.request, CapacityRoutes[0]));
  } finally {
    await server.shutdown();
  }
}

/** Proves a smaller Gateway receive carrier rejects the same otherwise-valid maximum vector. */
export async function runGatewayReceiveCapacityFuseMutation(): Promise<void> {
  const vector = (await capacityVectors())[0];
  if (vector === undefined) {
    throw new Error("capacity proof vector is absent");
  }
  const request = requestForRoute(vector.request, CapacityRoutes[0]);
  const service = new ProviderGatewayServiceShell({
    authenticator: {
      authenticate: async () => ({
        ok: true,
        serviceAccount: { namespace: "tetral", name: "agent-runtime", podUid: "pod_capacity" },
      }),
    },
    runtimeBindingTokenVerifier: { verify: () => true },
    ready: () => true,
    logger: { info: () => undefined, error: () => undefined },
    providerStreamer: { stream: (input) => successfulCapacityStream(input.request) },
    credentialResolver: capacityCredentialResolver(),
  });
  const server = new Server({
    "grpc.max_receive_message_length": 4 * 1024 * 1024,
    "grpc.max_send_message_length": 8 * 1024 * 1024,
  });
  const implementation: ProviderGatewayServiceServer = {
    streamProviderRequest(call) {
      void (async () => {
        try {
          for await (const event of service.streamProviderRequest(call.request, call.metadata)) {
            call.write(event);
          }
          call.end();
        } catch (error) {
          call.emit("error", error instanceof Error ? error : new Error("capacity mutation failed"));
        }
      })();
    },
    runWeb(_call, callback) {
      callback(new Error("not implemented"));
    },
  };
  server.addService(ProviderGatewayServiceService, implementation);
  const port = await new Promise<number>((resolve, reject) => {
    server.bindAsync("127.0.0.1:0", ServerCredentials.createInsecure(), (error, boundPort) => {
      if (error !== null) {
        reject(error);
        return;
      }
      resolve(boundPort);
    });
  });
  const client = new RuntimePodGatewayClient({
    address: `127.0.0.1:${port}`,
    tokenPath: "/unused",
    channelOptions: gatewayGrpcChannelOptions(),
    metadataFactory: async () => new Metadata(),
  });
  try {
      await collectGatewayEvents(client, request);
  } finally {
    await new Promise<void>((resolve) => server.tryShutdown(() => resolve()));
  }
}

/** Carries a large complete tool call through production Gateway transport and Runtime mapping. */
export async function runLargeToolInputMappingProof() {
  const inputs = largeMemoryToolInputs();
  const base = assembledVector("large_tool_input", [textMessage("large_tool_prompt", 1, "write it")]);
  const request = requestForRoute(base.request, CapacityRoutes[1]);
  const service = new ProviderGatewayServiceShell({
    authenticator: {
      authenticate: async () => ({
        ok: true,
        serviceAccount: { namespace: "tetral", name: "agent-runtime", podUid: "pod_capacity" },
      }),
    },
    runtimeBindingTokenVerifier: { verify: () => true },
    ready: () => true,
    logger: { info: () => undefined, error: () => undefined },
    providerStreamer: {
      stream: async function* () {
        for (const [index, input] of inputs.entries()) {
          yield {
            requestId: request.requestId,
            modelRequestId: request.modelRequestId,
            type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_CALL,
            toolCall: { id: `call_large_memory_${index}`, name: "memory", inputJson: JSON.stringify(input), metadataJson: "{}" },
          };
        }
        yield {
          requestId: request.requestId,
          modelRequestId: request.modelRequestId,
          type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
          finish: {
            reason: ProviderFinishReason.PROVIDER_FINISH_REASON_TOOL_CALLS,
            usage: { inputTotalTokens: 1, inputUncachedTokens: 1, outputTotalTokens: 1, totalTokens: 2, providerUsageJson: "{}" },
            metadataJson: "{}",
            contextWindowTokens: CapacityTargetTokens,
            outputTokenLimit: 128_000,
          },
        };
      },
    },
  });
  const server = createGatewayGrpcServer(service);
  const port = await server.bind("127.0.0.1:0");
  const client = new RuntimePodGatewayClient({
    address: `127.0.0.1:${port}`,
    tokenPath: "/unused",
    channelOptions: gatewayGrpcChannelOptions(),
    metadataFactory: async () => new Metadata(),
  });
  try {
    return await Effect.runPromise(Stream.runCollect(createLLMService(client).stream(request)));
  } finally {
    await server.shutdown();
  }
}

/** Replays the recorded GLM stream through the provider adapter, Gateway gRPC, and Runtime validator. */
export async function runRecordedGLMTransportProof() {
  const fixture = await readFile(new URL(
    "../../../../../gateway/packages/provider-gateway/test/golden/fixtures/zai-glm-5.2-live-2026-07-04.sse",
    import.meta.url,
  ), "utf8");
  const base = assembledVector("glm_recorded_stream", [textMessage("glm_recorded_prompt", 1, "use Search")]);
  const request: ProviderRequest = {
    ...base.request,
    requestId: "req_glm_recorded_stream",
    modelRequestId: "mreq_glm_recorded_stream",
    model: { providerId: "zai", modelId: "glm-5.2", variant: "" },
    tools: [{
      name: "Search",
      description: "Search.",
      function: {
        inputSchemaJson: JSON.stringify({
          type: "object",
          properties: { query: { type: "string" } },
          required: ["query"],
          additionalProperties: false,
        }),
      },
    }],
  };
  const providerRegistry = new ProviderClientRegistry({
    fetch: Object.assign(async () => new Response(fixture, {
      status: 200,
      headers: { "content-type": "text/event-stream" },
    }), { preconnect: () => undefined }),
  });
  const service = new ProviderGatewayServiceShell({
    authenticator: {
      authenticate: async () => ({
        ok: true,
        serviceAccount: { namespace: "tetral", name: "agent-runtime", podUid: "pod_glm_recorded_stream" },
      }),
    },
    runtimeBindingTokenVerifier: { verify: () => true },
    ready: () => true,
    logger: { info: () => undefined, error: () => undefined },
    providerStreamer: { stream: (input) => providerRegistry.stream(input) },
    credentialResolver: {
      resolve: async () => ({
        ok: true,
        credential: {
          source: "session",
          authType: "provider_api_key",
          providerId: "zai",
          supplyMode: "zai-api-key",
          vaultId: "vault_glm_recorded_stream",
          credentialId: "credential_glm_recorded_stream",
          accessMode: "api_key",
          apiKey: "fixture-zai-key",
        },
      }),
      recordPlatformFailure: () => undefined,
    } as unknown as ProviderCredentialResolver,
  });
  const server = createGatewayGrpcServer(service);
  const port = await server.bind("127.0.0.1:0");
  const client = new RuntimePodGatewayClient({
    address: `127.0.0.1:${port}`,
    tokenPath: "/unused",
    channelOptions: gatewayGrpcChannelOptions(),
    metadataFactory: async () => new Metadata(),
  });
  try {
    return Array.from(await Effect.runPromise(Stream.runCollect(createLLMService(client).stream(request))));
  } finally {
    server.server.forceShutdown();
  }
}

function largeMemoryToolInputs() {
  return [
    { action: "create", path: "notes/large.md", content: `CREATE_HEAD${"\u0001".repeat(9_000)}CREATE_TAIL` },
    {
      action: "replace",
      path: "notes/large.md",
      old_text: `OLD_HEAD${"<".repeat(5_000)}OLD_TAIL`,
      new_text: `NEW_HEAD${"\\".repeat(5_000)}NEW_TAIL`,
    },
  ] as const;
}

/** Carries an exact maximum Read envelope through the real formatter, projection, transport, and provider lowering. */
export async function runMaximumReadTransportProof() {
  const resultJson = maximumReadEnvelope();
  const digest = createHash("sha256").update(resultJson).digest("hex");
  const bridgeClient = {
    acceptSandboxExecution(
      _request: AcceptSandboxExecutionRequest,
      _metadata: Metadata,
      _options: CallOptions,
      callback: (error: Error | null, response?: unknown) => void,
    ) {
      callback(null, {
        ack: { status: BridgeWriteStatus.BRIDGE_WRITE_STATUS_COMMITTED, runtimeInputId: "", runtimeWriteId: "", errorCode: "" },
      });
      return noopUnaryCall();
    },
    awaitSandboxExecution(
      _request: AwaitSandboxExecutionRequest,
      _metadata: Metadata,
      callback: (error: Error | null, response?: unknown) => void,
    ) {
      callback(null, { resultJson, resultDigest: digest, backgroundTaskStarted: false, taskId: "" });
      return noopUnaryCall();
    },
  } as unknown as AgentRuntimeBridgeServiceClient;
  const runner = new RuntimePodToolRunner({
    bridgeAddress: "unused",
    webAddress: "unused",
    mcpConnectorAddress: "unused",
    tokenPath: "/unused",
    bridgeClient,
    webClient: {} as never,
    mcpConnectorClient: {} as never,
    metadataFactory: async () => new Metadata(),
  });
  const catalog = createToolCatalog({ family: "claude" });
  const entry = lookupToolEntry(catalog, "Read");
  if (entry === undefined) {
    throw new Error("maximum Read proof could not resolve the Read tool");
  }
  const input = { file_path: "notes/maximum.txt", offset: Number.MAX_SAFE_INTEGER, limit: 2000 };
  const toolRequest: RuntimeToolExecutionRequest = {
    workspaceId: "wksp_capacity",
    sessionId: "sesn_capacity",
    sessionThreadId: "thr_capacity",
    bindingId: "bind_capacity",
    bindingGeneration: 1,
    runtimeBindingToken: "binding-token",
    targetPodUid: "pod_capacity",
    modelRequestId: "mreq_maximum_read",
    modelToolCallId: "call_maximum_read",
    modelOrder: 0,
    toolUseEventId: "event_maximum_read",
    entry,
    input,
    currentModel: { providerId: "openai", modelId: "gpt-5.5" },
    committedMessages: [],
    abortSignal: new AbortController().signal,
  };
  const result = await runner.runTool(toolRequest);
  if (result.type !== "completed") {
    throw new Error(`maximum Read proof did not complete: ${result.type}`);
  }
  const runtimePart: RuntimePart = {
    id: "part_maximum_read",
    sessionId: "sesn_capacity",
    messageId: "message_maximum_read",
    sequence: 0,
    type: "tool",
    toolCallId: "call_maximum_read",
    toolName: "Read",
    toolUseEventId: "event_maximum_read",
    state: {
      status: "completed",
      input: { value: input, preview: "[bounded]", truncated: false },
      output: result.output,
    },
    createdAt,
    completedAt: createdAt,
  };
  const assembled = assembledVector(
    "maximum_read",
    [message("message_maximum_read", 1, "assistant", [runtimePart])],
    undefined,
    ["TETRAL_MAXIMUM_READ_HEAD", "TETRAL_MAXIMUM_READ_TAIL"],
  );
  const request = requestForRoute(assembled.request, CapacityRoutes[1]);
  let providerBody = "";
  const providerRegistry = new ProviderClientRegistry({
    fetch: Object.assign(async (input: RequestInfo | URL, init?: RequestInit) => {
      providerBody = await new Request(input, init).text();
      return new Response(JSON.stringify({ error: { type: "server_error", message: "synthetic capacity response" } }), {
        status: 500,
        headers: { "content-type": "application/json" },
      });
    }, { preconnect: () => undefined }),
  });
  const service = new ProviderGatewayServiceShell({
    authenticator: {
      authenticate: async () => ({
        ok: true,
        serviceAccount: { namespace: "tetral", name: "agent-runtime", podUid: "pod_capacity" },
      }),
    },
    runtimeBindingTokenVerifier: { verify: () => true },
    ready: () => true,
    logger: { info: () => undefined, error: () => undefined },
    providerStreamer: { stream: (providerInput) => providerRegistry.stream(providerInput) },
    credentialResolver: capacityCredentialResolver(),
  });
  const server = createGatewayGrpcServer(service);
  const port = await server.bind("127.0.0.1:0");
  const client = new RuntimePodGatewayClient({
    address: `127.0.0.1:${port}`,
    tokenPath: "/unused",
    channelOptions: gatewayGrpcChannelOptions(),
    metadataFactory: async () => new Metadata(),
  });
  try {
      await collectGatewayEvents(client, request);
  } finally {
    await server.shutdown();
  }
  const projected = toGatewayRuntimeMessages([message("message_maximum_read", 1, "assistant", [runtimePart])]);
  if (!projected.ok) {
    throw new Error("maximum Read proof did not project");
  }
  return {
    envelopeBytes: Buffer.byteLength(resultJson, "utf8"),
    providerRequestBytes: ProviderRequestMessage.encode(request).finish().byteLength,
    providerBodyContainsMarkers:
      providerBody.includes("TETRAL_MAXIMUM_READ_HEAD") && providerBody.includes("TETRAL_MAXIMUM_READ_TAIL"),
    projectedOutputBytes: Buffer.byteLength(projected.messages[0]?.parts[0]?.tool?.outputOrErrorJson ?? "", "utf8"),
    outputPreserved:
      projected.messages[0]?.parts[0]?.tool?.outputOrErrorJson === JSON.stringify({ text: result.output.text }),
    truncated: result.output.truncated,
  };
}

/** Transmits a real oversized event and returns Runtime's local receive-fuse classification. */
export async function runGatewayReceiveFuseProof() {
  const base = assembledVector("receive_fuse", [textMessage("receive_fuse_prompt", 1, "test")]);
  const request = requestForRoute(base.request, CapacityRoutes[0]);
  const implementation: ProviderGatewayServiceServer = {
    streamProviderRequest(call) {
      call.write({
        requestId: request.requestId,
        modelRequestId: request.modelRequestId,
        type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_CALL,
        toolCall: {
          id: "call_oversized",
          name: "oversized",
          inputJson: JSON.stringify({ content: "x".repeat(8 * 1024 * 1024) }),
          metadataJson: "{}",
        },
      });
      call.end();
    },
    runWeb(_call, callback) {
      callback(new Error("not implemented"));
    },
  };
  const server = new Server({
    "grpc.max_receive_message_length": 32 * 1024 * 1024,
    "grpc.max_send_message_length": 16 * 1024 * 1024,
  });
  server.addService(ProviderGatewayServiceService, implementation);
  const port = await new Promise<number>((resolve, reject) => {
    server.bindAsync("127.0.0.1:0", ServerCredentials.createInsecure(), (error, boundPort) => {
      if (error !== null) {
        reject(error);
        return;
      }
      resolve(boundPort);
    });
  });
  const client = new RuntimePodGatewayClient({
    address: `127.0.0.1:${port}`,
    tokenPath: "/unused",
    channelOptions: gatewayGrpcChannelOptions(),
    metadataFactory: async () => new Metadata(),
  });
  try {
      return await collectGatewayError(client, request);
  } finally {
    await new Promise<void>((resolve) => server.tryShutdown(() => resolve()));
  }
}

/** Exercises deterministic rejection and readiness failure through Gateway gRPC and Runtime LLM normalization. */
export async function runPreEventGatewayFailureProof() {
  const base = assembledVector("pre_event_failure", [textMessage("pre_event_prompt", 1, "test")]);
  const validRequest = requestForRoute(base.request, CapacityRoutes[0]);
  return {
    invalidArgument: await collectPreEventGatewayFailure({ ...validRequest, requestId: "" }, true),
    unavailable: await collectPreEventGatewayFailure(validRequest, false),
  };
}

/** Proves that the private wire's zero sentinel becomes delay absence at Runtime. */
export async function runGatewayAbsentRetryDelayProof() {
  const base = assembledVector("absent_retry_delay", [textMessage("absent_retry_delay_prompt", 1, "test")]);
  const request = requestForRoute(base.request, CapacityRoutes[0]);
  const service = new ProviderGatewayServiceShell({
    authenticator: {
      authenticate: async () => ({
        ok: true,
        serviceAccount: { namespace: "tetral", name: "agent-runtime", podUid: "pod_capacity" },
      }),
    },
    runtimeBindingTokenVerifier: { verify: () => true },
    ready: () => true,
    logger: { info: () => undefined, error: () => undefined },
    providerStreamer: {
      stream: async function* () {
        yield {
          requestId: request.requestId,
          modelRequestId: request.modelRequestId,
          type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR,
          providerError: {
            metadataJson: "{}",
            error: {
              code: "provider_unavailable",
              message: "Provider capacity is unavailable.",
              retryable: true,
              fatal: false,
              statusCode: 503,
              retryAfterMs: 0,
            },
          },
        };
      },
    },
  });
  const server = createGatewayGrpcServer(service);
  const port = await server.bind("127.0.0.1:0");
  const client = new RuntimePodGatewayClient({
    address: `127.0.0.1:${port}`,
    tokenPath: "/unused",
    channelOptions: gatewayGrpcChannelOptions(),
    metadataFactory: async () => new Metadata(),
  });
  try {
    const events = Array.from(await Effect.runPromise(Stream.runCollect(createLLMService(client).stream(request))));
    const terminal = events.at(-1);
    if (terminal?.type !== "provider-error") {
      throw new Error("absent retry delay proof did not receive provider error");
    }
    return terminal.error;
  } finally {
    server.server.forceShutdown();
  }
}

async function collectPreEventGatewayFailure(request: ProviderRequest, ready: boolean) {
  const service = new ProviderGatewayServiceShell({
    authenticator: {
      authenticate: async () => ({
        ok: true,
        serviceAccount: { namespace: "tetral", name: "agent-runtime", podUid: "pod_capacity" },
      }),
    },
    runtimeBindingTokenVerifier: { verify: () => true },
    ready: () => ready,
    logger: { info: () => undefined, error: () => undefined },
  });
  const server = createGatewayGrpcServer(service);
  const port = await server.bind("127.0.0.1:0");
  const client = new RuntimePodGatewayClient({
    address: `127.0.0.1:${port}`,
    tokenPath: "/unused",
    channelOptions: gatewayGrpcChannelOptions(),
    metadataFactory: async () => new Metadata(),
  });
  try {
    return await Effect.runPromise(Stream.runCollect(createLLMService(client).stream(request)).pipe(Effect.flip));
  } finally {
    server.server.forceShutdown();
  }
}

function requestForRoute(request: ProviderRequest, route: CapacityRoute): ProviderRequest {
  return {
    ...request,
    requestId: `${request.requestId}_${route.family}`,
    modelRequestId: `${request.modelRequestId}_${route.family}`,
    model: { providerId: route.providerId, modelId: route.modelId, variant: "" },
    // The capacity matrix exercises ordinary declarations on every transport;
    // the OpenAI-only freeform declaration remains on its sole supported route.
    tools: route.family === "openai" ? request.tools : request.tools.filter((tool) => tool.freeform === undefined),
  };
}

function capacityCredentialResolver(): ProviderCredentialResolver {
  return {
    resolve: async (request: Pick<ProviderRequest, "model">) => {
      const route = CapacityRoutes.find((candidate) =>
        candidate.providerId === request.model?.providerId && candidate.modelId === request.model.modelId);
      if (route === undefined) {
        throw new Error("capacity proof route is absent");
      }
      return {
        ok: true,
        credential: {
          source: "session",
          authType: "provider_api_key",
          providerId: route.providerId,
          supplyMode: route.supplyMode,
          vaultId: "vault_capacity",
          credentialId: "credential_capacity",
          accessMode: "api_key",
          apiKey: "capacity-provider-key",
        },
      };
    },
    recordPlatformFailure: () => undefined,
  } as unknown as ProviderCredentialResolver;
}

async function* successfulCapacityStream(request: ProviderRequest) {
  yield {
    requestId: request.requestId,
    modelRequestId: request.modelRequestId,
    type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
    finish: {
      reason: ProviderFinishReason.PROVIDER_FINISH_REASON_STOP,
      usage: {
        inputTotalTokens: 1,
        inputUncachedTokens: 1,
        outputTotalTokens: 1,
        totalTokens: 2,
        providerUsageJson: "{}",
      },
      metadataJson: "{}",
      contextWindowTokens: CapacityTargetTokens,
      outputTokenLimit: 128_000,
    },
  };
}

async function capacityVectors(): Promise<readonly {
  readonly name: string;
  readonly estimatedTokens: number;
  readonly request: ProviderRequest;
  readonly wireMarkers: readonly string[];
  readonly loadedContextBytes?: number;
}[]> {
  const ordinaryMarkers = ["TETRAL_ORDINARY_FIRST", "TETRAL_ORDINARY_LAST"] as const;
  const ordinary = fitMessagesToTokenTarget((budget) => [
    textMessage("ordinary_ascii", 1, markedText(Math.floor(budget / 2), ordinaryMarkers[0], "", "x")),
    textMessage("ordinary_utf8", 2, markedText(budget - Math.floor(budget / 2), "", ordinaryMarkers[1], "界")),
  ]);
  const toolPart = completedToolPart();
  const toolMarkers = ["TETRAL_TOOL_INPUT_FIRST", "TETRAL_TOOL_INPUT_LAST", "TETRAL_TOOL_OUTPUT_FIRST", "TETRAL_TOOL_OUTPUT_LAST"] as const;
  const withTools = fitMessagesToTokenTarget((budget) => [
    textMessage("tool_context", 1, "x".repeat(budget)),
    message("tool_result", 2, "assistant", [toolPart]),
  ]);
  const mcpMarkers = ["TETRAL_MCP_FIRST", "TETRAL_MCP_LAST"] as const;
  const withCatalog = fitMessagesToTokenTarget((budget) => [textMessage("catalog_context", 1, "x".repeat(budget))]);
  const fragmentMarkers = ["TETRAL_FRAGMENT_FIRST", "TETRAL_FRAGMENT_LAST"] as const;
  const fragmented = fitMessagesToTokenTarget((budget) => {
    const count = 512;
    const each = Math.floor(budget / count);
    return Array.from({ length: count }, (_, index) => {
      const length = each + (index === count - 1 ? budget - each * count : 0);
      return textMessage(`fragment_${index}`, index + 1, markedText(
        length,
        index === 0 ? fragmentMarkers[0] : "",
        index === count - 1 ? fragmentMarkers[1] : "",
        "x",
      ));
    });
  });
  const escapeDenseMarkers = ["TETRAL_ESCAPE_OUTPUT_FIRST", "TETRAL_ESCAPE_OUTPUT_LAST"] as const;
  const escapeDenseToolMessages = Array.from({ length: 48 }, (_, index) =>
    message(`escape_tool_${index}`, index + 1, "assistant", [escapeDenseToolPart(index, 48, escapeDenseMarkers)]));
  const escapeDenseOutputs = fitMessagesToTokenTarget((budget) => [
    ...escapeDenseToolMessages,
    textMessage("escape_dense_filler", escapeDenseToolMessages.length + 1, "x".repeat(budget)),
  ]);
  const loadedEscapeDense = await loadCapacityContext(escapeDenseOutputs);

  return [
    assembledVector("ordinary_ascii_and_utf8", ordinary, undefined, ordinaryMarkers),
    assembledVector("escaped_tool_input_and_max_output", withTools, undefined, toolMarkers),
    assembledVector("maximum_mcp_catalog", withCatalog, maximumMcpCatalog(), mcpMarkers),
    assembledVector("fragmented_history", fragmented, undefined, fragmentMarkers),
    {
      ...assembledVector("escape_dense_output_history", loadedEscapeDense.messages, undefined, escapeDenseMarkers),
      loadedContextBytes: loadedEscapeDense.contextBytes,
    },
  ];
}

async function loadCapacityContext(messages: readonly RuntimeMessage[]) {
  const contextJson = JSON.stringify({
    messages: messages.map((message, index) => ({
      ...message,
      owningEventId: `evt_capacity_context_${index}`,
      eventSequence: index + 1,
    })),
    turnFacts: { events: [], messageLineage: [] },
    coldCoverage: {
      pendingToolIds: [],
      pendingSandboxExecutionIds: [],
      pendingAttachmentIdentities: [],
      undeliveredMailDeliveryIds: [],
    },
    thread: {
      parentThreadId: null,
      role: "main",
      visibility: "public",
      taskName: null,
      agentType: "general",
      status: "idle",
    },
  });
  const client = {
    loadContext(
      _request: unknown,
      _metadata: Metadata,
      callback: (error: Error | null, response?: unknown) => void,
    ) {
      callback(null, {
        ack: { status: BridgeWriteStatus.BRIDGE_WRITE_STATUS_COMMITTED },
        contextJson,
        runtimeBindingToken: "binding-token",
      });
      return noopUnaryCall();
    },
  } as unknown as AgentRuntimeBridgeServiceClient;
  const loader = new BridgeAPIContextLoader({
    address: "unused",
    tokenPath: "/unused",
    client,
    metadataFactory: async () => new Metadata(),
  });
  const loaded = await loader.loadThreadContext({
    requestId: "req_capacity_context",
    workspaceId: "wksp_capacity",
    sessionId: "sesn_capacity",
    sessionThreadId: "thr_capacity",
    bindingId: "bind_capacity",
    bindingGeneration: 1,
    targetPodUid: "pod_capacity",
    runtimeInputId: "rin_capacity_context",
    eventIds: [],
    sequenceFrom: 0,
    sequenceTo: 0,
  });
  return { messages: loaded.messages, contextBytes: Buffer.byteLength(contextJson, "utf8") };
}

function assembledVector(
  name: string,
  runtimeMessages: readonly RuntimeMessage[],
  toolCatalog: ToolCatalog | undefined = undefined,
  wireMarkers: readonly string[] = [],
) {
  const projected = toGatewayRuntimeMessages(runtimeMessages);
  if (!projected.ok) {
    throw new Error(`capacity vector ${name} did not project`);
  }
  const assembled = assembleProviderCallRequest({
    identity: {
      workspaceId: "wksp_capacity",
      sessionId: "sesn_capacity",
      sessionThreadId: "thr_capacity",
      bindingId: "bind_capacity",
      bindingGeneration: 1,
      targetPodUid: "pod_capacity",
      runtimeBindingToken: "binding-token",
    },
    requestId: `req_${name}`,
    modelRequestId: `mreq_${name}`,
    currentModel: { providerId: "openai", modelId: "gpt-5.5" },
    runtimeMessages: projected.messages,
    runtime: {
      requestKind: ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
      systemInstructions: `${SystemWireMarkers[0]} Operate within the declared tool and context contract.`,
      agentSystem: `${SystemWireMarkers[1]} Operate as the capacity-proof agent.`,
      memoryStores: [{
        memoryStoreId: "mem_capacity",
        name: "capacity-memory",
        access: "read_only",
        instructions: `${SystemWireMarkers[2]} retain the capacity proof marker`,
      }],
      skillsIndex: [{
        skillId: "skill_capacity",
        skillVersionId: "skv_capacity",
        version: "1",
        name: "capacity-skill",
        description: `${SystemWireMarkers[3]} retain the capacity proof marker`,
        directory: "capacity-skill",
      }],
      skillGuidanceDescriptionBudgetBytes: 8 * 1024,
      ...(toolCatalog === undefined ? {} : { toolCatalog }),
      maxOutputTokens: 128_000,
      timeoutMs: 30_000,
    },
  });
  if (!assembled.ok) {
    throw new Error(`capacity vector ${name} did not assemble: ${assembled.error.code}: ${assembled.error.message}`);
  }
  return {
    name,
    estimatedTokens: estimatedRuntimeMessagesTokens(runtimeMessages),
    request: assembled.request,
    wireMarkers: [...SystemWireMarkers, ...wireMarkers],
  };
}

function fitMessagesToTokenTarget(build: (characterBudget: number) => readonly RuntimeMessage[]): readonly RuntimeMessage[] {
  let budget = CapacityTargetTokens * 4;
  let messages = build(budget);
  for (let attempt = 0; attempt < 4; attempt += 1) {
    const estimated = estimatedRuntimeMessagesTokens(messages);
    const delta = CapacityTargetTokens - estimated;
    if (delta >= 0 && delta <= 2) {
      return messages;
    }
    budget = Math.max(1, budget + delta * 4);
    messages = build(budget);
  }
  return messages;
}

function maximumMcpCatalog() {
  return createToolCatalog({
    family: "gpt",
    mcpManifests: Array.from({ length: 20 }, (_, serverIndex) => ({
      mcpServerName: `capacity-server-${serverIndex}`,
      manifestETag: `etag-${serverIndex}`,
      manifestGeneration: 1,
      tools: Array.from({ length: 4 }, (_, toolIndex) => ({
        name: `capacity_${serverIndex}_${toolIndex}`,
        description: markedText(
          60 * 1024,
          serverIndex === 0 && toolIndex === 0 ? "TETRAL_MCP_FIRST" : "",
          serverIndex === 19 && toolIndex === 3 ? "TETRAL_MCP_LAST" : "",
          "d",
        ),
        inputSchema: { type: "object", properties: { value: { type: "string" } } },
      })),
    })),
  });
}

function completedToolPart(): RuntimePart {
  const inputLength = 102_400;
  const input = {
    old_text: markedText(inputLength, "TETRAL_TOOL_INPUT_FIRST", "", "\u0001"),
    new_text: markedText(inputLength, "", "TETRAL_TOOL_INPUT_LAST", "\u0001"),
  };
  const outputLength = 512 * 1024 - Buffer.byteLength(JSON.stringify({ text: "" }), "utf8");
  const output = markedText(outputLength, "TETRAL_TOOL_OUTPUT_FIRST", "TETRAL_TOOL_OUTPUT_LAST", "x");
  return {
    id: "part_tool",
    sessionId: "sesn_capacity",
    messageId: "tool_result",
    sequence: 0,
    type: "tool",
    toolCallId: "call_capacity",
    toolName: "replace",
    toolUseEventId: "event_capacity",
    state: {
      status: "completed",
      input: { value: input, preview: "[bounded]", truncated: true },
      output: { text: output, truncated: false },
    },
    createdAt,
    completedAt: createdAt,
  };
}

function escapeDenseToolPart(
  index: number,
  count: number,
  markers: readonly [string, string],
): RuntimePart {
  const output = markedText(
    80 * 1024,
    index === 0 ? markers[0] : "",
    index === count - 1 ? markers[1] : "",
    "\u0001",
  );
  return {
    id: `part_escape_tool_${index}`,
    sessionId: "sesn_capacity",
    messageId: `escape_tool_${index}`,
    sequence: 0,
    type: "tool",
    toolCallId: `call_escape_${index}`,
    toolName: "Read",
    toolUseEventId: `event_escape_${index}`,
    state: {
      status: "completed",
      input: { value: { index }, preview: `{\"index\":${index}}`, truncated: false },
      output: { text: output, truncated: false },
    },
    createdAt,
    completedAt: createdAt,
  };
}

function markedText(length: number, prefix: string, suffix: string, fill: string): string {
  const remaining = length - prefix.length - suffix.length;
  if (remaining < 0) {
    throw new Error("capacity marker exceeds its fixture budget");
  }
  return `${prefix}${fill.repeat(remaining)}${suffix}`;
}

function maximumReadEnvelope(): string {
  const headMarker = "TETRAL_MAXIMUM_READ_HEAD";
  const tailMarker = "TETRAL_MAXIMUM_READ_TAIL";
  const lineBreaks = "\n".repeat(1999);
  const envelope = (content: string): string => JSON.stringify({
    status: "success",
    result: {
      content,
      start_line: Number.MAX_SAFE_INTEGER,
      returned_lines: 2000,
      truncated: false,
      line_truncations: 0,
    },
  });
  const empty = envelope(headMarker + tailMarker + lineBreaks);
  let remaining = 200_000 - Buffer.byteLength(empty, "utf8");
  if (remaining < 0) {
    throw new Error("maximum Read envelope metadata exceeds its byte budget");
  }
  let escapeDense = "\\\"".repeat(Math.floor(remaining / 4));
  remaining %= 4;
  if (remaining >= 2) {
    escapeDense += "\\";
    remaining -= 2;
  }
  if (remaining === 1) {
    escapeDense += "x";
  }
  const result = envelope(headMarker + escapeDense + tailMarker + lineBreaks);
  if (Buffer.byteLength(result, "utf8") !== 200_000) {
    throw new Error("maximum Read envelope does not match the 200000-byte contract");
  }
  return result;
}

function noopUnaryCall(): ClientUnaryCall {
  return { cancel() {} } as ClientUnaryCall;
}

async function collectGatewayEvents(
  client: RuntimePodGatewayClient,
  request: ProviderRequest,
): Promise<readonly ProviderStreamEvent[]> {
  const handle = await client.streamProviderRequest(request);
  const events = Array.from(await Effect.runPromise(Stream.runCollect(handle.events)));
  const completion = await handle.completion;
  if (completion.outcome === "transport_failure") {
    throw completion.error;
  }
  if (completion.outcome !== "eof") {
    throw new Error(`Gateway stream did not complete normally: ${completion.outcome}`);
  }
  return events;
}

async function collectGatewayError(
  client: RuntimePodGatewayClient,
  request: ProviderRequest,
): Promise<GatewayClientError> {
  try {
    await collectGatewayEvents(client, request);
  } catch (error) {
    return error as GatewayClientError;
  }
  throw new Error("Gateway stream unexpectedly completed normally");
}

function textMessage(id: string, sequence: number, text: string): RuntimeMessage {
  return message(id, sequence, "user", [{
    id: `part_${id}`,
    sessionId: "sesn_capacity",
    messageId: id,
    sequence: 0,
    type: "text",
    text,
    truncated: false,
    status: "completed",
    createdAt,
    completedAt: createdAt,
  }]);
}

function message(id: string, sequence: number, role: "user" | "assistant", parts: readonly RuntimePart[]): RuntimeMessage {
  return RuntimeMessageSchema.parse({
    id,
    sessionId: "sesn_capacity",
    role,
    origin: role === "user" ? "user" : "agent",
    sequence,
    status: "completed",
    createdAt,
    parts,
  });
}

export function capacityProofHasRequiredHeadroom(measurement: GatewayCapacityMeasurement): boolean {
  return measurement.estimatedTokens >= CapacityTargetTokens - 2 &&
    measurement.estimatedTokens <= CapacityTargetTokens &&
    measurement.encodedRequestBytes === measurement.receivedRequestBytes &&
    measurement.headroomRatio >= RequiredHeadroomRatio;
}

if (import.meta.main) {
  process.stdout.write(`${JSON.stringify(await runGatewayCapacityProof())}\n`);
}
