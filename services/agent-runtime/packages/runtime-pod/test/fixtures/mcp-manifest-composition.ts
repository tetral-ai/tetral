import { readFile } from "node:fs/promises";
import { Metadata } from "@grpc/grpc-js";
import { Context, Effect, Exit, Layer, Scope } from "effect";
import * as SessionManager from "@tetral/agent-runtime-core/src/session/session-manager.js";
import * as ThreadLoop from "@tetral/agent-runtime-core/src/thread-loop/thread-loop.js";
import { ThreadRuntime } from "@tetral/agent-runtime-core/src/thread-loop/thread-runtime.js";
import type { RuntimeThreadPreloadState } from "@tetral/agent-runtime-core/src/thread-loop/thread-state.js";
import { lookupToolEntry } from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import type { ToolCatalog } from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import { BridgeWriteStatus } from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type { AgentRuntimeBridgeServiceClient } from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type { ApplyRuntimeConfigRequest, ApplyRuntimeConfigResponse } from "@tetral/agent-runtime-protocol/src/gen/tetral/agent_runtime/v1/agent_runtime.js";
import { BridgeAPIContextLoader } from "../../src/bridge-client.js";
import { runtimeMCPManifestEligibilityFromPatchPayloads, runtimeToolPolicyForThread } from "../../src/command.js";
import { RuntimeControlService } from "../../src/runtime-service.js";
import type { RuntimeSessionRunHost } from "../../src/runtime-service.js";
import { RuntimePodToolRunner } from "../../src/tool-runner.js";
import { RecordingContextLoader, runtimeThreadLoopLayer, testRunCustody } from "../../../core/test/unit/thread-loop/thread-loop-test-support.js";
import { buildThreadLoopUserMessage } from "../../../core/test/unit/runtime-message-builders.js";

interface CompositionInput {
  readonly workspaceId: string;
  readonly sessionId: string;
  readonly runtimeConfigPayloadJson: string;
  readonly readyManifestPayloadJson: string;
  readonly runtimeCommandRequest: ApplyRuntimeConfigRequest;
  readonly coldContextJson: string;
  readonly coldRuntimeBindingToken: string;
  readonly readyGeneration: number;
  readonly unreadyGeneration: number;
  readonly toolName: string;
}

const inputPath = process.argv[2];
if (inputPath === undefined) {
  throw new Error("composition input path is required");
}
const input = JSON.parse(await readFile(inputPath, "utf8")) as CompositionInput;

const warm = await applyWarmTransition(input);
assertCatalogState(input, warm.events[0], warm.catalogs[0], true);
assertCatalogState(input, warm.events[1], warm.catalogs[1], false);
assertCatalogState(input, warm.events[2], warm.catalogs[2], false);
if (
  warm.commandResponse.applied === undefined ||
  warm.events[1]?.source !== "runtime_config_update" ||
  warm.events[1].disposition !== "applied" ||
  warm.events[2]?.disposition !== "stale" ||
  warm.events[2].currentGeneration !== input.unreadyGeneration
) {
  throw new Error(
    "warm manifest generation did not apply or reject stale restoration through runtime config control",
  );
}

const cold = await loadReplacement(input);
if (
  cold.events.length !== 1 ||
  cold.events[0]?.source !== "cold_load" ||
  cold.events[0].currentGeneration !== input.unreadyGeneration
) {
  throw new Error(
    "replacement runtime did not cold-load the durable unready generation",
  );
}
assertCatalogState(input, cold.events[0], cold.catalogs[0], false);

const warmStaleToolProof = await proveNextProviderRejectsStaleTool(input, warm.catalogs[2]!);
const coldStaleToolProof = await proveNextProviderRejectsStaleTool(input, cold.catalogs[0]!);

process.stdout.write(
  JSON.stringify({
    commandResponse: warm.commandResponse,
    warmCurrentGeneration: warm.events[2]?.currentGeneration,
    coldCurrentGeneration: cold.events[0]?.currentGeneration,
    warmMcpConnectorCalls: warmStaleToolProof.mcpConnectorCalls,
    warmNextProviderRequests: warmStaleToolProof.providerRequests,
    coldMcpConnectorCalls: coldStaleToolProof.mcpConnectorCalls,
    coldNextProviderRequests: coldStaleToolProof.providerRequests,
  }),
);

async function proveNextProviderRejectsStaleTool(
  input: CompositionInput,
  toolCatalog: ToolCatalog,
): Promise<{ readonly mcpConnectorCalls: number; readonly providerRequests: number }> {
  let mcpConnectorCalls = 0;
  const providerToolNames: string[][] = [];
  const runner = new RuntimePodToolRunner({
    bridgeAddress: "unused.test:9090",
    webAddress: "unused.test:9090",
    mcpConnectorAddress: "unused.test:9091",
    tokenPath: "/unused/token",
    metadataFactory: async () => new Metadata(),
    mcpConnectorClient: {
      runMcpTool: () => {
        mcpConnectorCalls += 1;
        throw new Error("stale MCP tool reached the connector route");
      },
    } as never,
  });
  const loader = new RecordingContextLoader([], {
    type: "messages",
    messages: [buildThreadLoopUserMessage("msg_stale_manifest", 1, "use the removed tool")],
  });
  const thread = new ThreadRuntime({
    workspaceId: input.workspaceId,
    sessionId: input.sessionId,
    sessionThreadId: `thrd_${input.sessionId}`,
    bindingId: "bind_manifest_composition",
    bindingGeneration: 1,
    targetPodUid: "pod_manifest_composition",
    runtimeBindingToken: "runtime-binding-token",
  });
  const result = await Effect.runPromise(Effect.gen(function* () {
    const threadLoop = yield* ThreadLoop.Service;
    return yield* threadLoop.run(thread, testRunCustody());
  }).pipe(Effect.provide(runtimeThreadLoopLayer(loader, {
    events: [{
      type: "tool-call",
      id: "stale-manifest-tool-call",
      toolName: input.toolName,
      input: { query: "tetral" },
      inputPreview: { preview: "{}", truncated: false },
    }, { type: "finish", finishReason: "tool-calls" }],
    providerCallRuntime: {
      systemInstructions: "manifest composition provider request",
      toolCatalog,
      toolsetFamily: "claude",
    },
    approvalMode: "full_access",
    onStream: (request) => providerToolNames.push(request.tools.map((tool) => tool.name)),
    runTool: (request) => runner.runTool(request),
  }))));
  if (result.type !== "completed" || providerToolNames.length < 1) {
    throw new Error("next provider request did not complete through ThreadLoop");
  }
  if (providerToolNames.some((names) => names.includes(input.toolName))) {
    throw new Error("removed MCP tool remained in the next provider request");
  }
  if (mcpConnectorCalls !== 0) {
    throw new Error("removed MCP tool reached the connector route");
  }
  return { mcpConnectorCalls, providerRequests: providerToolNames.length };
}

async function applyWarmTransition(
  input: CompositionInput,
): Promise<{
  readonly events: SessionManager.RuntimeMCPManifestUpdateEvent[];
  readonly catalogs: ToolCatalog[];
  readonly commandResponse: ApplyRuntimeConfigResponse;
}> {
  const { manager, scope, events, catalogs } = await buildManager();
  try {
    const preload = await Effect.runPromise(
      manager.preloadThread(
        preloadState(input, [
          manifestPatch(
            input,
            input.readyGeneration,
            input.readyManifestPayloadJson,
            "etag_ready",
          ),
        ]),
      ),
    );
    if (!preload.ok || !preload.applied) {
      throw new Error("warm runtime preload failed");
    }
    const service = runtimeControlService(input.runtimeCommandRequest, manager);
    const commandResponse = await service.applyRuntimeConfig(input.runtimeCommandRequest, new Metadata());
    if (commandResponse.applied === undefined) {
      throw new Error("warm runtime command host rejected the manifest update");
    }
    const staleResponse = await service.applyRuntimeConfig({
      ...input.runtimeCommandRequest,
      sessionConfig: undefined,
      mcpManifest: {
        mcpServerName: input.runtimeCommandRequest.mcpManifest?.mcpServerName ?? "github",
        generation: input.readyGeneration,
        contentJson: input.readyManifestPayloadJson,
      },
    }, new Metadata());
    if (staleResponse.duplicate === undefined) {
      throw new Error("warm runtime command host rejected the stale replay envelope");
    }
    return { events, catalogs, commandResponse };
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
  }
}

async function loadReplacement(
  input: CompositionInput,
): Promise<{ readonly events: SessionManager.RuntimeMCPManifestUpdateEvent[]; readonly catalogs: ToolCatalog[] }> {
  const { manager, scope, events, catalogs } = await buildManager();
  try {
    const loader = new BridgeAPIContextLoader({
      address: "unused.test",
      tokenPath: "/unused/token",
      metadataFactory: async () => new Metadata(),
      client: {
        loadContext: (_request: unknown, _metadata: Metadata, callback: (error: Error | null, response: unknown) => void) => {
          callback(null, {
            ack: { status: BridgeWriteStatus.BRIDGE_WRITE_STATUS_COMMITTED },
            contextJson: input.coldContextJson,
            runtimeBindingToken: input.coldRuntimeBindingToken,
          });
        },
      } as unknown as AgentRuntimeBridgeServiceClient,
    });
    const loaded = await loader.loadThreadContext(controlState(input, "rin_manifest_cold_load"));
    const preload = await Effect.runPromise(
      manager.preloadThread(
        {
          ...controlState(input, "rin_manifest_cold_load"),
          runtimeBindingToken: loaded.runtimeBindingToken,
          messages: loaded.messages,
          thread: loaded.thread,
          ...(loaded.runtimeConfigPatch !== undefined ? { runtimeConfigPatch: loaded.runtimeConfigPatch } : {}),
          ...(loaded.mcpManifests !== undefined ? { mcpManifests: loaded.mcpManifests } : {}),
          coldCoverage: loaded.coldCoverage,
        },
      ),
    );
    if (!preload.ok || !preload.applied) {
      throw new Error("replacement runtime preload failed");
    }
    return { events, catalogs };
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
  }
}

async function buildManager(): Promise<{
  readonly manager: SessionManager.Interface;
  readonly scope: Scope.Scope;
  readonly events: SessionManager.RuntimeMCPManifestUpdateEvent[];
  readonly catalogs: ToolCatalog[];
}> {
  const events: SessionManager.RuntimeMCPManifestUpdateEvent[] = [];
  const catalogs: ToolCatalog[] = [];
  const threadLoopLayer = Layer.succeed(
    ThreadLoop.Service,
    ThreadLoop.Service.of({
      run: () => Effect.succeed({ type: "completed", modelMessageCount: 0 }),
      closeFailedRun: () => Effect.succeed({ type: "landed" }),
      seedRuntimeModel: () => undefined,
      installLoadedPendingToolUses: () => Effect.succeed({ ok: true }),
      installLoadedSandboxExecutions: () => Effect.succeed({ ok: true }),
    }),
  );
  const managerLayer = SessionManager.layer({
    maxLocalSessions: 1,
    now: () => "2026-08-10T12:00:00.000Z",
    recordMCPManifestUpdate: (event) => events.push(event),
    resolveMCPManifestEligibility: (patches, serverName) => {
      const payloads = patches.map((patch) => patch.contentJson);
      catalogs.push(runtimeToolPolicyForThread(undefined, payloads, "claude").toolCatalog);
      return runtimeMCPManifestEligibilityFromPatchPayloads(payloads, serverName);
    },
  }).pipe(Layer.provide(threadLoopLayer));
  return await Effect.runPromise(
    Effect.gen(function* () {
      const scope = yield* Scope.make();
      const context = yield* Layer.buildWithScope(managerLayer, scope);
      return {
        manager: Context.get(context, SessionManager.Service),
        scope,
        events,
        catalogs,
      };
    }),
  );
}

function runtimeControlService(
  request: ApplyRuntimeConfigRequest,
  manager: SessionManager.Interface,
): RuntimeControlService {
  const runHost = {
    handleRuntimeConfigPatch: async (sessionId: string, command: Parameters<SessionManager.Interface["applyRuntimeConfigPatch"]>[1]) =>
      await Effect.runPromise(manager.applyRuntimeConfigPatch(sessionId, command)),
  } as unknown as RuntimeSessionRunHost;
  return new RuntimeControlService({
    ownPod: {
      namespace: "engine",
      name: "runtime-pod-composition",
      uid: request.targetPodUid,
      ip: "127.0.0.1",
    },
    allowedBridge: { namespace: "tetral-system", name: "bridge" },
    authenticator: {
      authenticate: async () => ({
        ok: true as const,
        serviceAccount: { namespace: "tetral-system", name: "bridge" },
      }),
    },
    runHost,
    cleanupController: {
      startCleanup: async (scope) => ({
        ok: true as const,
        sessionId: scope.sessionId,
        completion: Promise.resolve({ ok: true as const, sessionId: scope.sessionId, cleaned: true }),
      }),
    },
    logger: { info: () => undefined, error: () => undefined },
    ready: () => true,
  });
}

function preloadState(
  input: CompositionInput,
  manifests: RuntimeThreadPreloadState["mcpManifests"],
): RuntimeThreadPreloadState {
  return {
    ...controlState(input, "rin_preload"),
    runtimeBindingToken: "runtime-binding-token",
    messages: [],
    runtimeConfigPatch: {
      ...addressState(input),
      configIdentity: "session:1",
      generation: 1,
      coldLoad: true,
      installedBuiltinFamily: "claude",
      contentJson: input.runtimeConfigPayloadJson,
    },
    mcpManifests: manifests,
    coldCoverage: {
      pendingToolIds: [],
      pendingSandboxExecutionIds: [],
      pendingAttachmentIdentities: [],
      undeliveredMailDeliveryIds: [],
    },
  };
}

function manifestPatch(
  input: CompositionInput,
  generation: number,
  payloadJson: string,
  manifestETag?: string,
) {
  return {
    ...addressState(input),
    configIdentity: `mcp:github:${generation}`,
    generation,
    mcpServerName: "github",
    ...(manifestETag !== undefined ? { manifestETag } : {}),
    contentJson: payloadJson,
  };
}

function addressState(input: CompositionInput) {
  return {
    workspaceId: input.workspaceId,
    sessionId: input.sessionId,
    sessionThreadId: `thrd_${input.sessionId}`,
    bindingId: "bind_manifest_composition",
    bindingGeneration: 1,
    targetPodUid: "pod_manifest_composition",
  };
}

function controlState(input: CompositionInput, runtimeInputId: string) {
  return {
    ...addressState(input),
    requestId: `req_${runtimeInputId}`,
    runtimeInputId,
    eventIds: [],
    sequenceFrom: 0,
    sequenceTo: 0,
  };
}

function assertCatalogState(
  input: CompositionInput,
  event: SessionManager.RuntimeMCPManifestUpdateEvent | undefined,
  catalog: ToolCatalog | undefined,
  expectedPresent: boolean,
): void {
  if (event === undefined || catalog === undefined) {
    throw new Error("manifest application event is missing");
  }
  const present = lookupToolEntry(catalog, input.toolName) !== undefined;
  if (present !== expectedPresent || event.toolCatalogEligible !== expectedPresent) {
    throw new Error(
      `MCP tool presence ${present} does not match expected ${expectedPresent}`,
    );
  }
}
