import { createHash } from "node:crypto";
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
import { createLLMService } from "../../../core/src/llm/llm-service.js";

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
}

const CapacityRoutes = [
  { family: "anthropic", providerId: "anthropic", modelId: "claude-opus-4-8", supplyMode: "anthropic-api-key" },
  { family: "openai", providerId: "openai", modelId: "gpt-5.5", supplyMode: "openai-api-key" },
  { family: "openai-compatible", providerId: "deepseek", modelId: "deepseek-v4-pro", supplyMode: "deepseek-api-key" },
] as const;

type CapacityRoute = (typeof CapacityRoutes)[number];

/** Runs the catalog-capacity proof through production projection, assembly, transport, and lowering. */
export async function runGatewayCapacityProof(): Promise<readonly GatewayCapacityMeasurement[]> {
  const vectors = capacityVectors();
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
        const events = await Effect.runPromise(Stream.runCollect(client.streamProviderRequest(request)));
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
      });
    }
    return measurements;
  } finally {
    await server.shutdown();
  }
}

/** Proves a smaller Runtime request carrier rejects a production-path maximum vector. */
export async function runGatewayCapacityFuseMutation(): Promise<void> {
  const vector = capacityVectors()[0];
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
  }));
  const port = await server.bind("127.0.0.1:0");
  const client = new RuntimePodGatewayClient({
    address: `127.0.0.1:${port}`,
    tokenPath: "/unused",
    channelOptions: { ...gatewayGrpcChannelOptions(), "grpc.max_send_message_length": 4 * 1024 * 1024 },
    metadataFactory: async () => new Metadata(),
  });
  try {
    await Effect.runPromise(Stream.runCollect(client.streamProviderRequest(requestForRoute(vector.request, CapacityRoutes[0]))));
  } finally {
    await server.shutdown();
  }
}

/** Carries a large complete tool call through production Gateway transport and Runtime mapping. */
export async function runLargeToolInputMappingProof() {
  const input = { content: "\u0001".repeat(200_000), file_path: "notes/large.txt" };
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
        yield {
          requestId: request.requestId,
          modelRequestId: request.modelRequestId,
          type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_CALL,
          toolCall: { id: "call_large", name: "Write", inputJson: JSON.stringify(input), metadataJson: "{}" },
        };
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
    ["TETRAL_MAXIMUM_READ"],
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
    await Effect.runPromise(Stream.runCollect(client.streamProviderRequest(request)));
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
    providerBodyContainsMarker: providerBody.includes("TETRAL_MAXIMUM_READ"),
    projectedOutputBytes: Buffer.byteLength(projected.messages[0]?.parts[0]?.tool?.outputOrErrorJson ?? "", "utf8"),
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
    return await Effect.runPromise(Stream.runCollect(client.streamProviderRequest(request)).pipe(Effect.flip));
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

function capacityVectors(): readonly {
  readonly name: string;
  readonly estimatedTokens: number;
  readonly request: ProviderRequest;
  readonly wireMarkers: readonly string[];
}[] {
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

  return [
    assembledVector("ordinary_ascii_and_utf8", ordinary, undefined, ordinaryMarkers),
    assembledVector("escaped_tool_input_and_max_output", withTools, undefined, toolMarkers),
    assembledVector("maximum_mcp_catalog", withCatalog, maximumMcpCatalog(), mcpMarkers),
    assembledVector("fragmented_history", fragmented, undefined, fragmentMarkers),
  ];
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
    if (Math.abs(delta) <= 2) {
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
  const outputLength = 256 * 1024 - Buffer.byteLength(JSON.stringify({ text: "" }), "utf8");
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

function markedText(length: number, prefix: string, suffix: string, fill: string): string {
  const remaining = length - prefix.length - suffix.length;
  if (remaining < 0) {
    throw new Error("capacity marker exceeds its fixture budget");
  }
  return `${prefix}${fill.repeat(remaining)}${suffix}`;
}

function maximumReadEnvelope(): string {
  const marker = "TETRAL_MAXIMUM_READ";
  const lineBreaks = "\n".repeat(1999);
  const envelope = (content: string): string => JSON.stringify({
    status: "success",
    result: {
      content,
      start_line: Number.MAX_SAFE_INTEGER,
      returned_lines: 2000,
      truncated: true,
      line_truncations: 0,
    },
  });
  const empty = envelope(marker + lineBreaks);
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
  const result = envelope(marker + escapeDense + lineBreaks);
  if (Buffer.byteLength(result, "utf8") !== 200_000) {
    throw new Error("maximum Read envelope does not match the 200000-byte contract");
  }
  return result;
}

function noopUnaryCall(): ClientUnaryCall {
  return { cancel() {} } as ClientUnaryCall;
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
    measurement.estimatedTokens <= CapacityTargetTokens + 2 &&
    measurement.encodedRequestBytes === measurement.receivedRequestBytes &&
    measurement.headroomRatio >= RequiredHeadroomRatio;
}

if (import.meta.main) {
  process.stdout.write(`${JSON.stringify(await runGatewayCapacityProof())}\n`);
}
