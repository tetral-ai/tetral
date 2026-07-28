/**
 * @packageDocumentation
 * Boots the Runtime Pod process and composes its Runtime Core, Bridge, Gateway, tool, authentication,
 * metrics, and gRPC dependencies. Bun executes this module as the service entrypoint, while tests
 * call its builders and policy parsers directly. Startup validates configuration and authentication
 * material before readiness opens, the assembled hosts share one scoped Runtime Core lifetime, and shutdown
 * stops the app before closing that scope. Runtime policy helpers turn ordered cold-load and patch
 * payloads into the tool catalog and provider behavior consumed by Agent Loop.
 */
import type { Metadata } from "@grpc/grpc-js";
import { RuntimeMessageStore } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import type {
  RuntimeInternalToolRepairCommit,
  RuntimeMessageInfo,
  RuntimeMessageStoreOperationControls,
  RuntimeMessageStoreWritePartResult,
  RuntimePart,
} from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import { DefaultProviderCallRuntimeConfig } from "@tetral/agent-runtime-core/src/agent-loop/provider-call-assembly.js";
import type { MemoryStorePromptEntry, SkillGuidanceIndexEntry } from "@tetral/agent-runtime-core/src/agent-loop/provider-call-assembly.js";
import { createApprovalReviewerToolCatalog, createToolCatalog } from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import type { ToolApprovalMode } from "@tetral/agent-runtime-core/src/tools/tool-gate.js";
import type { InstalledBuiltinFamily, MCPManifest, MCPManifestToolConfig, ToolCatalog, ToolConfig, ToolPermissionPolicy } from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import type { RuntimeThreadRoleState } from "@tetral/agent-runtime-core/src/session/session-state.js";
import { createLLMService } from "@tetral/agent-runtime-core/src/llm/llm-service.js";
import { buildOutboundBearerMetadata, KubernetesTokenReviewClient, validateKubernetesTokenReviewReviewerMaterial } from "./auth.js";
import type { RuntimeTokenReviewClient, ServiceAccountTokenConfig } from "./auth.js";
import { BridgeAPIApprovalReviewerThreadCreator, BridgeAPIContextLoader, BridgeAPIControlInputCommitter, BridgeAPIEventWriter, BridgeAPIInternalToolRepairCommitter, BridgeAPITaskNotificationCommitter } from "./bridge-client.js";
import { buildRuntimeCoreHosts } from "./core-hosts.js";
import type { RuntimeCoreHosts } from "./core-hosts.js";
import { createRuntimeApprovalReviewer, loadApprovalReviewerAssets } from "./approval-reviewer.js";
import { RuntimePodGatewayClient } from "./gateway-client.js";
import { gatewayGrpcChannelOptions } from "./bounds.js";
import { loadRuntimePodConfigFromProcessEnv, parseModelRef } from "./config.js";
import type { RuntimePodConfig, RuntimePodModelRef } from "./config.js";
import { createRuntimePodApp } from "./app.js";
import type { RuntimePodApp } from "./app.js";
import { createJsonLogger, runtimeCloseoutLogRecord, startupFailureLogRecord } from "./logger.js";
import type { RuntimePodLogger } from "./logger.js";
import type { RuntimeControlInputCommitter, RuntimeTaskNotificationCommitter } from "./runtime-service.js";
import { RuntimePodToolRunner } from "./tool-runner.js";
import { RuntimePodMetricsRegistry } from "./metrics.js";
import type { RuntimePodMetricsSource } from "./metrics.js";

/** Owns the process-lifetime dependencies started and stopped by the Runtime Pod command. */
export interface RuntimePodCommandDependencies {
  readonly tokenReviewClient: RuntimeTokenReviewClient;
  readonly coreHosts: RuntimeCoreHosts;
  readonly app: RuntimePodApp;
  readonly metrics: RuntimePodMetricsSource;
}

/**
 * Supplies process-level overrides for startup logging, dependency assembly, signal registration,
 * and the terminal wait used by the executable and its tests.
 */
export interface RuntimePodCommandOptions {
  readonly logger?: RuntimePodLogger;
  readonly dependencyBuilder?: (input: {
    readonly config: RuntimePodConfig;
    readonly logger: RuntimePodLogger;
  }) => Promise<RuntimePodCommandDependencies>;
  readonly waitForever?: () => Promise<never>;
  readonly registerSignalHandlers?: (shutdown: () => Promise<void>) => void;
}

/**
 * Supplies focused construction seams while the default dependency builder retains ownership of
 * the complete production object graph.
 */
export interface RuntimePodDependencyBuilderOptions {
  readonly coreHostsFactory?: typeof buildRuntimeCoreHosts;
  readonly tokenReviewClientFactory?: (config: RuntimePodConfig) => RuntimeTokenReviewClient;
  readonly outboundMetadataFactory?: (config: ServiceAccountTokenConfig) => Promise<Metadata>;
  readonly controlInputCommitterFactory?: (input: {
    readonly config: RuntimePodConfig;
    readonly metadataFactory: (config: ServiceAccountTokenConfig) => Promise<Metadata>;
  }) => RuntimeControlInputCommitter;
  readonly taskNotificationCommitterFactory?: (input: {
    readonly config: RuntimePodConfig;
    readonly metadataFactory: (config: ServiceAccountTokenConfig) => Promise<Metadata>;
  }) => RuntimeTaskNotificationCommitter;
}

function providerStreamTimeoutOptions(config: Pick<RuntimePodConfig, "providerStreamTimeoutMs">): {
  readonly providerCallRuntime: { readonly timeoutMs: number };
  readonly compaction: { readonly timeoutMs: number };
} {
  return {
    providerCallRuntime: { timeoutMs: config.providerStreamTimeoutMs },
    compaction: { timeoutMs: config.providerStreamTimeoutMs },
  };
}

/**
 * Loads process configuration, builds dependencies, registers graceful shutdown, starts the gRPC
 * app, and then waits for process termination. Startup failures are logged with bounded records and
 * rethrown without dependency details.
 */
export async function runRuntimePodCommand(options: RuntimePodCommandOptions = {}): Promise<void> {
  const startupLogger = options.logger ?? createJsonLogger({ write: (line) => process.stderr.write(line) });
  const config = loadRuntimePodConfigFromProcessEnv();
  if (!config.ok) {
    startupLogger.error(startupFailureLogRecord(config.error));
    throw new Error("runtime pod config error");
  }
  const logger =
    options.logger ??
    createJsonLogger({
      write: (line) => process.stderr.write(line),
      deploymentEnvironment: config.config.deploymentEnvironment,
      serviceVersion: config.config.serviceVersion,
    });
  let dependencies: RuntimePodCommandDependencies;
  try {
    dependencies = await (options.dependencyBuilder ?? buildRuntimePodCommandDependencies)({
      config: config.config,
      logger,
    });
  } catch (error) {
    logger.error(startupFailureLogRecord({ kind: "startup_error", message: "runtime pod startup failed", cause: error }));
    throw new Error("runtime pod startup error");
  }
  const shutdown = async (): Promise<void> => {
    await dependencies.app.shutdown();
    await dependencies.coreHosts.close();
  };
  (options.registerSignalHandlers ?? registerProcessSignalHandlers)(shutdown);
  await dependencies.app.start();
  await (options.waitForever ?? waitForever)();
}

/**
 * Assembles one Runtime Pod dependency graph from validated configuration. The graph wires Runtime
 * Core to authenticated Bridge and Gateway adapters, shares active thread scopes across context,
 * event, tool, and reviewer operations, and returns the app and scoped hosts as one lifetime unit.
 */
export async function buildRuntimePodCommandDependencies(input: {
  readonly config: RuntimePodConfig;
  readonly logger: RuntimePodLogger;
  readonly builderOptions?: RuntimePodDependencyBuilderOptions;
}): Promise<RuntimePodCommandDependencies> {
  const outboundMetadataFactory = input.builderOptions?.outboundMetadataFactory ?? buildOutboundBearerMetadata;
  const metrics = new RuntimePodMetricsRegistry();
  const bridgeContextLoader = new BridgeAPIContextLoader({
    address: input.config.bridgeApiGrpcAddress,
    tokenPath: input.config.outboundInternalGrpcTokenPath,
    metadataFactory: outboundMetadataFactory,
  });
  const gatewayClient = new RuntimePodGatewayClient({
    address: input.config.gatewayGrpcAddress,
    tokenPath: input.config.outboundInternalGrpcTokenPath,
    metadataFactory: outboundMetadataFactory,
    channelOptions: gatewayGrpcChannelOptions(),
  });
  const approvalReviewerToolCatalog = createApprovalReviewerToolCatalog();
  const approvalReviewerAssets = loadApprovalReviewerAssets();
  const approvalReviewerThreadCreator = new BridgeAPIApprovalReviewerThreadCreator({
    address: input.config.bridgeApiGrpcAddress,
    tokenPath: input.config.outboundInternalGrpcTokenPath,
    metadataFactory: outboundMetadataFactory,
    releaseThreadScope: (sessionId, sessionThreadId) => bridgeContextLoader.releaseAcceptedInputForThread(sessionId, sessionThreadId),
  });
  const internalToolRepairCommitter = new BridgeAPIInternalToolRepairCommitter({
    address: input.config.bridgeApiGrpcAddress,
    tokenPath: input.config.outboundInternalGrpcTokenPath,
    metadataFactory: outboundMetadataFactory,
    scopeForThread: (sessionId, sessionThreadId) => bridgeContextLoader.acceptedInputForThread(sessionId, sessionThreadId),
  });
  let subAgentRunHost: RuntimeCoreHosts["subAgentRunHost"] | undefined;
  const toolRunner = new RuntimePodToolRunner({
    bridgeAddress: input.config.bridgeApiGrpcAddress,
    webAddress: input.config.webConnectorGrpcAddress,
    mcpConnectorAddress: input.config.mcpConnectorGrpcAddress,
    tokenPath: input.config.outboundInternalGrpcTokenPath,
    metadataFactory: outboundMetadataFactory,
    scopeForThread: (sessionId, sessionThreadId) => bridgeContextLoader.acceptedInputForThread(sessionId, sessionThreadId),
    subAgentRunHost: () => subAgentRunHost,
  });
  const streamTimeoutOptions = providerStreamTimeoutOptions(input.config);
  const createRuntimeId = (prefix: string): string => `${prefix}_${crypto.randomUUID()}`;
  const coreHosts = await (input.builderOptions?.coreHostsFactory ?? buildRuntimeCoreHosts)({
    maxLocalSessions: 256,
    maxConcurrentTools: 8,
    now: () => new Date().toISOString(),
    contextLoader: bridgeContextLoader,
    registerAcceptedInput: (input) => bridgeContextLoader.registerAcceptedInput(input),
    metrics,
    recordCloseoutEvent: (event) => {
      try {
        metrics.recordCloseoutEvent(event);
        const record = runtimeCloseoutLogRecord(event);
        if (event.event === "runtime_closeout_recovered") {
          input.logger.info(record);
        } else {
          input.logger.error(record);
        }
      } catch {
        // A metrics or logging sink cannot participate in closeout custody.
      }
    },
    agentLoop: {
      messageStore: new RuntimeHotMessageStore(internalToolRepairCommitter),
      sessionEventWriter: new BridgeAPIEventWriter({
        address: input.config.bridgeApiGrpcAddress,
        tokenPath: input.config.outboundInternalGrpcTokenPath,
        metadataFactory: outboundMetadataFactory,
        scopeForThread: (sessionId, sessionThreadId) => bridgeContextLoader.acceptedInputForThread(sessionId, sessionThreadId),
      }),
      runtime: {
        now: () => new Date().toISOString(),
        monotonicMs: () => Date.now(),
        createId: createRuntimeId,
        sleep: async (durationMs, signal) =>
          await new Promise<boolean>((resolve) => {
            if (signal.aborted) {
              resolve(false);
              return;
            }
            const timeout = setTimeout(() => resolve(true), durationMs);
            signal.addEventListener("abort", () => {
              clearTimeout(timeout);
              resolve(false);
            }, { once: true });
        }),
      },
      llmService: createLLMService(gatewayClient),
      storeOperationTimeoutMs: 5_000,
      providerCallRuntime: {
        ...DefaultProviderCallRuntimeConfig,
        ...streamTimeoutOptions.providerCallRuntime,
        approvalReviewerPolicy: approvalReviewerAssets.policyPrompt,
        skillGuidanceDescriptionBudgetBytes: input.config.skillGuidance.descriptionBudgetBytes,
      },
      compaction: streamTimeoutOptions.compaction,
      approvalMode: "ask_for_approval",
      runtimeModel: (session) =>
        runtimeModelForThread(
          session.identity.threadRole,
          session.state.runtimeConfigPatches().map((patch) => patch.payloadJson),
          input.config.platformModels.approvalReviewer,
        ),
      runtimePolicy: (session) =>
        runtimeToolPolicyForThread(
          session.identity.threadRole,
          session.state.runtimeConfigPatches().map((patch) => patch.payloadJson),
          session.state.installedBuiltinFamily(),
          approvalReviewerToolCatalog,
        ),
      runTool: (request) => toolRunner.runTool(request),
      reviewApproval: createRuntimeApprovalReviewer(() => subAgentRunHost, {
        model: input.config.platformModels.approvalReviewer,
        threadCreator: approvalReviewerThreadCreator,
        createId: createRuntimeId,
        registerCommitScope: (command) => bridgeContextLoader.registerScopedAcceptedInput(command),
        logger: input.logger,
        assets: approvalReviewerAssets,
      }),
    },
  });
  subAgentRunHost = coreHosts.subAgentRunHost;
  const tokenReviewClient =
    input.builderOptions?.tokenReviewClientFactory?.(input.config) ??
    new KubernetesTokenReviewClient({
      apiServerUrl: input.config.kubernetesApiServerUrl,
      reviewerTokenPath: input.config.tokenReviewReviewerTokenPath,
      apiServerCaCertPath: input.config.kubernetesApiCaCertPath,
    });
  const taskNotificationCommitter =
    input.builderOptions?.taskNotificationCommitterFactory?.({ config: input.config, metadataFactory: outboundMetadataFactory }) ??
    new BridgeAPITaskNotificationCommitter({
      address: input.config.bridgeApiGrpcAddress,
      tokenPath: input.config.outboundInternalGrpcTokenPath,
      metadataFactory: outboundMetadataFactory,
    });
  const controlInputCommitter =
    input.builderOptions?.controlInputCommitterFactory?.({ config: input.config, metadataFactory: outboundMetadataFactory }) ??
    new BridgeAPIControlInputCommitter({
      address: input.config.bridgeApiGrpcAddress,
      tokenPath: input.config.outboundInternalGrpcTokenPath,
      metadataFactory: outboundMetadataFactory,
    });
  const app = createRuntimePodApp({
    config: input.config,
    logger: input.logger,
    tokenReviewClient,
    commandRunHost: coreHosts.commandRunHost,
    acceptedInputRegistrar: {
      registerAcceptedInput: (command) => bridgeContextLoader.registerAcceptedInput({ ...command, kind: "messages" as const }),
    },
    controlInputCommitter,
    taskNotificationCommitter,
    cleanupRunHost: coreHosts.cleanupRunHost,
    shutdownActiveRuns: coreHosts.shutdownActiveRuns,
    metrics,
    bootstrap: {
      runtime: async () => {
        return;
      },
      core: async () => undefined,
      authClient: async () => {
        await validateKubernetesTokenReviewReviewerMaterial({
          reviewerTokenPath: input.config.tokenReviewReviewerTokenPath,
          apiServerCaCertPath: input.config.kubernetesApiCaCertPath,
        });
        await outboundMetadataFactory({ tokenPath: input.config.outboundInternalGrpcTokenPath });
      },
    },
  });
  return { app, coreHosts, metrics, tokenReviewClient };
}

async function waitForever(): Promise<never> {
  return await new Promise<never>(() => undefined);
}

function registerProcessSignalHandlers(shutdown: () => Promise<void>): void {
  process.once("SIGTERM", () => {
    void shutdown().then(() => process.exit(0));
  });
  process.once("SIGINT", () => {
    void shutdown().then(() => process.exit(0));
  });
}

/**
 * Builds the standard-thread runtime policy from one cold-load payload. The payload must identify
 * one installed builtin tool family; malformed required policy structures throw rather than
 * producing a partially configured catalog.
 */
export function runtimeToolPolicyFromPatchPayload(
  payloadJson: string | undefined,
): {
  readonly approvalMode: ToolApprovalMode;
  readonly system?: string;
  readonly toolCatalog: ToolCatalog;
  readonly skillsIndex?: readonly SkillGuidanceIndexEntry[];
  readonly memoryStores?: readonly MemoryStorePromptEntry[];
  readonly providerRescheduleBudget: number;
  readonly compactionRescheduleBudget: number;
} {
  return runtimeToolPolicyFromPatchPayloads(payloadJson === undefined ? [] : [payloadJson]);
}

/**
 * Folds ordered runtime policy payloads into one standard-thread policy. The first payload supplies
 * the cold installed builtin family. Later scalar fields replace their prior values; a patch that
 * carries either approval mode or tool configs also replaces the config slice, using an empty slice
 * when only approval mode is present. Ready MCP manifests with a matching active MCP toolset
 * configuration accumulate into the resulting catalog.
 */
export function runtimeToolPolicyFromPatchPayloads(
  payloadJsons: readonly string[],
): ReturnType<typeof runtimeToolPolicyFromPatchPayloadsWithFamily> {
  return runtimeToolPolicyFromPatchPayloadsWithFamily(payloadJsons, undefined, true);
}

function runtimeToolPolicyFromPatchPayloadsWithFamily(
  payloadJsons: readonly string[],
  installedBuiltinFamily: InstalledBuiltinFamily | undefined,
  deriveInstalledBuiltinFamily: boolean,
): {
  readonly approvalMode: ToolApprovalMode;
  readonly system?: string;
  readonly toolCatalog: ToolCatalog;
  readonly skillsIndex?: readonly SkillGuidanceIndexEntry[];
  readonly memoryStores?: readonly MemoryStorePromptEntry[];
  readonly providerRescheduleBudget: number;
  readonly compactionRescheduleBudget: number;
} {
  let approvalMode: ToolApprovalMode = "ask_for_approval";
  let family = installedBuiltinFamily;
  let configs: readonly ToolConfig[] = [];
  let mcpToolsets: ReadonlyMap<string, MCPManifestToolsetConfig> | undefined;
  const mcpManifests: MCPManifest[] = [];
  let skillsIndex: readonly SkillGuidanceIndexEntry[] | undefined;
  let memoryStores: readonly MemoryStorePromptEntry[] | undefined;
  let system: string | undefined;
  let providerRescheduleBudget = 3;
  let compactionRescheduleBudget = 2;
  for (const [patchIndex, payloadJson] of payloadJsons.entries()) {
    const parsed = parseRuntimePolicyPayload(payloadJson);
    if (parsed === undefined) {
      continue;
    }
    const runtimeConfig = recordField(parsed, "runtime_config") ?? recordField(parsed, "runtimeConfig");
    if (deriveInstalledBuiltinFamily && patchIndex === 0) {
      family = unambiguousColdInstalledBuiltinFamily(runtimeConfig);
    }
    const systemPatch = runtimeAgentSystemPatch(parsed, runtimeConfig);
    if (systemPatch.present) {
      system = systemPatch.value;
    }
    const nextSkillsIndexValue = recordField(runtimeConfig, "skillsIndex") ?? recordField(runtimeConfig, "skills_index");
    if (nextSkillsIndexValue !== undefined) {
      skillsIndex = parseSkillGuidanceIndex(nextSkillsIndexValue);
    }
    const nextMemoryStoresValue = recordField(runtimeConfig, "memoryStores") ??
      recordField(runtimeConfig, "memory_stores") ??
      recordField(parsed, "memoryStores") ??
      recordField(parsed, "memory_stores");
    if (nextMemoryStoresValue !== undefined) {
      memoryStores = parseMemoryStores(nextMemoryStoresValue);
    }
    providerRescheduleBudget = parseRescheduleBudget(
      recordNumberField(runtimeConfig, "providerRescheduleBudget") ?? recordNumberField(runtimeConfig, "provider_reschedule_budget"),
      providerRescheduleBudget,
    );
    compactionRescheduleBudget = parseRescheduleBudget(
      recordNumberField(runtimeConfig, "compactionRescheduleBudget") ?? recordNumberField(runtimeConfig, "compaction_reschedule_budget"),
      compactionRescheduleBudget,
    );
    const nextApprovalMode = parseApprovalMode(
      parsed.approval_mode ??
        parsed.approvalMode ??
        recordField(parsed.tool_policy, "approval_mode") ??
        recordField(parsed.toolPolicy, "approvalMode"),
    );
    const configValues =
      recordArrayField(parsed.tools, "configs") ??
      recordArrayField(parsed.tool_policy, "configs") ??
      recordArrayField(recordField(parsed.toolPolicy, "tools"), "configs") ??
      recordArrayField(recordField(parsed.tool_policy, "tools"), "configs");
    if (nextApprovalMode !== undefined || configValues !== undefined) {
      approvalMode = nextApprovalMode ?? approvalMode;
      configs = parseToolConfigs(configValues ?? []);
    }
    const nextMcpToolsets = parseMcpToolsets(
      recordArrayField(parsed.tool_policy, "mcpToolsets") ??
        recordArrayField(parsed.tool_policy, "mcp_toolsets") ??
        recordArrayField(parsed.toolPolicy, "mcpToolsets") ??
        recordArrayField(parsed.toolPolicy, "mcp_toolsets"),
    );
    if (nextMcpToolsets !== undefined) {
      mcpToolsets = nextMcpToolsets;
    }
    const mcpManifest = parseMcpManifest(parsed);
    if (mcpManifest !== undefined) {
      const configuredManifest = applyMcpToolsetConfig(mcpManifest, mcpToolsets);
      if (configuredManifest !== undefined) {
        mcpManifests.push(configuredManifest);
      }
    }
  }
  return {
    approvalMode,
    ...(system !== undefined ? { system } : {}),
    ...(skillsIndex !== undefined ? { skillsIndex } : {}),
    ...(memoryStores !== undefined ? { memoryStores } : {}),
    providerRescheduleBudget,
    compactionRescheduleBudget,
    toolCatalog: createToolCatalog({
      includeSubAgentTools: true,
      family: requiredColdInstalledBuiltinFamily(family),
      configs,
      mcpManifests,
    }),
  };
}

function parseMemoryStores(value: unknown): readonly MemoryStorePromptEntry[] {
  if (!Array.isArray(value)) {
    throw new Error("runtime memory stores are malformed");
  }
  return value.map((item): MemoryStorePromptEntry => {
    if (!isRecord(item)) {
      throw new Error("runtime memory stores are malformed");
    }
    const memoryStoreId = recordStringField(item, "memoryStoreId") ?? recordStringField(item, "memory_store_id");
    const name = recordStringField(item, "name");
    const access = recordStringField(item, "access");
    const instructions = item.instructions;
    if (
      memoryStoreId === undefined ||
      name === undefined ||
      (access !== "read_write" && access !== "read_only") ||
      (instructions !== undefined && instructions !== null && (typeof instructions !== "string" || instructions.length === 0))
    ) {
      throw new Error("runtime memory stores are malformed");
    }
    return {
      memoryStoreId,
      name,
      access,
      ...(typeof instructions === "string" ? { instructions } : {}),
    };
  });
}

function requiredColdInstalledBuiltinFamily(family: InstalledBuiltinFamily | undefined): InstalledBuiltinFamily {
  if (family === undefined) {
    throw new Error("runtime installed builtin family is malformed");
  }
  return family;
}

function unambiguousColdInstalledBuiltinFamily(runtimeConfig: unknown): InstalledBuiltinFamily {
  if (!isRecord(runtimeConfig)) {
    throw new Error("runtime installed builtin family is malformed");
  }
  const installedTools = recordField(runtimeConfig, "installedTools") ?? recordField(runtimeConfig, "installed_tools");
  if (!Array.isArray(installedTools)) {
    throw new Error("runtime installed builtin family is malformed");
  }
  let family: InstalledBuiltinFamily | undefined;
  for (const tool of installedTools) {
    if (!isRecord(tool) || tool.type !== "tetral_agent_toolset") {
      continue;
    }
    if (family !== undefined || (tool.family !== "claude" && tool.family !== "gpt")) {
      throw new Error("runtime installed builtin family is malformed");
    }
    family = tool.family;
  }
  if (family === undefined) {
    throw new Error("runtime installed builtin family is malformed");
  }
  return family;
}

/**
 * Selects the effective policy for a thread. Approval-reviewer threads receive their isolated
 * full-access reviewer catalog; every other role uses the supplied installed family and ordered
 * runtime patches to construct the normal catalog and provider budgets.
 */
export function runtimeToolPolicyForThread(
  threadRole: RuntimeThreadRoleState | undefined,
  payloadJsons: readonly string[],
  installedBuiltinFamily: InstalledBuiltinFamily | undefined,
  approvalReviewerToolCatalog: ToolCatalog = createApprovalReviewerToolCatalog(),
): {
  readonly approvalMode: ToolApprovalMode;
  readonly system?: string;
  readonly toolCatalog: ToolCatalog;
  readonly skillsIndex?: readonly SkillGuidanceIndexEntry[];
  readonly memoryStores?: readonly MemoryStorePromptEntry[];
  readonly providerRescheduleBudget: number;
  readonly compactionRescheduleBudget: number;
} {
  if (threadRole === "approval_reviewer") {
    return {
      approvalMode: "full_access",
      toolCatalog: approvalReviewerToolCatalog,
      providerRescheduleBudget: 3,
      compactionRescheduleBudget: 2,
    };
  }
  return runtimeToolPolicyFromPatchPayloadsWithFamily(payloadJsons, installedBuiltinFamily, false);
}

/**
 * Resolves the thread model from immutable Runtime configuration. Reviewer threads use the
 * configured reviewer model; all other roles inspect the cold agent config payload. Malformed or
 * absent configuration returns undefined so Agent Loop can settle the run through its model gate.
 */
export function runtimeModelForThread(
  threadRole: RuntimeThreadRoleState | undefined,
  payloadJsons: readonly string[],
  reviewerModel: RuntimePodModelRef,
): RuntimePodModelRef | undefined {
  if (threadRole === "approval_reviewer") {
    return reviewerModel;
  }
  for (const payloadJson of payloadJsons) {
    let payload: unknown;
    try {
      payload = JSON.parse(payloadJson);
    } catch {
      continue;
    }
    if (!isRecord(payload)) {
      continue;
    }
    const runtimeConfig = recordField(payload, "runtime_config");
    if (!isRecord(runtimeConfig)) {
      continue;
    }
    const agent = recordField(runtimeConfig, "agent");
    if (!isRecord(agent)) {
      continue;
    }
    const config = recordField(agent, "config");
    if (!isRecord(config)) {
      continue;
    }
    const model = recordField(config, "model");
    if (typeof model !== "string" || model.length === 0) {
      continue;
    }
    return parseModelRef(model);
  }
  return undefined;
}

function runtimeAgentSystemPatch(
  payload: Record<string, unknown>,
  runtimeConfig: unknown,
): { readonly present: false } | { readonly present: true; readonly value?: string } {
  for (const source of [payload, runtimeConfig]) {
    if (!isRecord(source) || !Object.prototype.hasOwnProperty.call(source, "system")) {
      continue;
    }
    const value = source.system;
    if (value === null) {
      return { present: true };
    }
    if (typeof value !== "string" || value.length === 0) {
      throw new Error("runtime agent system is malformed");
    }
    return { present: true, value };
  }
  return { present: false };
}

function parseRescheduleBudget(value: number | undefined, fallback: number): number {
  if (value === undefined) {
    return fallback;
  }
  if (!Number.isSafeInteger(value) || value < 0 || value > 10) {
    throw new Error("runtime reschedule budget is malformed");
  }
  return value;
}

function parseSkillGuidanceIndex(value: unknown): readonly SkillGuidanceIndexEntry[] {
  if (!Array.isArray(value)) {
    throw new Error("runtime skill guidance index is malformed");
  }
  return value.map((item): SkillGuidanceIndexEntry => {
    if (!isRecord(item)) {
      throw new Error("runtime skill guidance index is malformed");
    }
    return {
      skillId: requiredRuntimeString(item, "skill_id"),
      skillVersionId: requiredRuntimeString(item, "skill_version_id"),
      version: requiredRuntimeString(item, "version"),
      name: requiredRuntimeString(item, "name"),
      description: optionalRuntimeString(item, "description"),
      directory: requiredRuntimeString(item, "directory"),
    };
  });
}

function requiredRuntimeString(record: Readonly<Record<string, unknown>>, field: string): string {
  const value = record[field];
  if (typeof value !== "string" || value.length === 0) {
    throw new Error("runtime skill guidance index is malformed");
  }
  return value;
}

function optionalRuntimeString(record: Readonly<Record<string, unknown>>, field: string): string {
  const value = record[field];
  if (typeof value !== "string") {
    throw new Error("runtime skill guidance index is malformed");
  }
  return value;
}

interface MCPManifestToolsetConfig {
  readonly mcpServerName: string;
  readonly defaultConfig?: MCPManifest["defaultConfig"] | undefined;
  readonly configs?: readonly MCPManifestToolConfig[] | undefined;
}

interface RuntimeInternalToolRepairCommitter {
  readonly commitInternalToolRepair: (repair: RuntimeInternalToolRepairCommit) => Promise<RuntimeMessageStoreWritePartResult>;
}

class RuntimeHotMessageStore extends RuntimeMessageStore {
  constructor(private readonly internalToolRepairCommitter: RuntimeInternalToolRepairCommitter) {
    super();
  }

  protected async writeMessageRecord(message: RuntimeMessageInfo, _controls: RuntimeMessageStoreOperationControls): Promise<unknown> {
    return {
      ok: true,
      messageId: message.id,
      operation: "writeMessage",
    };
  }

  protected async writePartRecord(part: RuntimePart, _controls: RuntimeMessageStoreOperationControls): Promise<unknown> {
    return {
      ok: true,
      messageId: part.messageId,
      partId: part.id,
      operation: "writePart",
    };
  }

  protected async commitInternalToolRepairRecord(repair: RuntimeInternalToolRepairCommit, _controls: RuntimeMessageStoreOperationControls): Promise<unknown> {
    return await this.internalToolRepairCommitter.commitInternalToolRepair(repair);
  }
}

function parseRuntimePolicyPayload(payloadJson: string | undefined): Record<string, unknown> | undefined {
  if (payloadJson === undefined || payloadJson.length === 0) {
    return undefined;
  }
  try {
    const parsed = JSON.parse(payloadJson) as unknown;
    return isRecord(parsed) ? parsed : undefined;
  } catch {
    return undefined;
  }
}

function parseApprovalMode(value: unknown): ToolApprovalMode | undefined {
  return value === "full_access" || value === "ask_for_approval" || value === "approve_for_me" ? value : undefined;
}

function parseMcpManifest(payload: Record<string, unknown>): MCPManifest | undefined {
  const manifest = recordField(payload, "mcp_manifest") ?? recordField(payload, "mcpManifest");
  if (manifest === undefined) {
    return undefined;
  }
  if (!isRecord(manifest)) {
    throw new Error("invalid MCP manifest runtime config payload");
  }
  const mcpServerName = recordStringField(manifest, "mcp_server_name") ?? recordStringField(manifest, "mcpServerName");
  const manifestETag = recordStringField(manifest, "manifest_etag") ?? recordStringField(manifest, "manifestETag");
  const manifestGeneration = recordNumberField(manifest, "manifest_generation") ?? recordNumberField(manifest, "manifestGeneration");
  const rawToolsValue = recordField(manifest, "tools");
  const rawTools = recordArrayField(manifest, "tools");
  const readiness = recordStringField(manifest, "readiness") ?? "ready";
  const diagnostic = recordStringField(manifest, "diagnostic");
  const rawReadiness = recordField(manifest, "readiness");
  const rawDiagnostic = recordField(manifest, "diagnostic");
  const etagPresent = "manifest_etag" in manifest || "manifestETag" in manifest;
  const readinessShape = rawReadiness === undefined || typeof rawReadiness === "string";
  const diagnosticShape = rawDiagnostic === undefined || rawDiagnostic === null || typeof rawDiagnostic === "string";
  if (readiness === "unready") {
    if (mcpServerName === undefined || etagPresent || manifestGeneration === undefined ||
      !Number.isSafeInteger(manifestGeneration) || manifestGeneration <= 0 || diagnostic === undefined ||
      !readinessShape || !diagnosticShape ||
      (rawToolsValue !== undefined && (!Array.isArray(rawToolsValue) || rawToolsValue.length > 0))) {
      throw new Error("invalid MCP manifest runtime config payload");
    }
    return undefined;
  }
  if (mcpServerName === undefined || manifestETag === undefined || manifestGeneration === undefined ||
    !Number.isSafeInteger(manifestGeneration) || manifestGeneration <= 0 || rawTools === undefined || readiness !== "ready" ||
    !readinessShape || !diagnosticShape || diagnostic !== undefined) {
    throw new Error("invalid MCP manifest runtime config payload");
  }
  return {
    mcpServerName,
    manifestETag,
    manifestGeneration,
    tools: parseMcpManifestTools(rawTools),
  };
}

function parseMcpToolsets(values: readonly unknown[] | undefined): ReadonlyMap<string, MCPManifestToolsetConfig> | undefined {
  if (values === undefined) {
    return undefined;
  }
  const toolsets = new Map<string, MCPManifestToolsetConfig>();
  for (const value of values) {
    if (!isRecord(value)) {
      throw new Error("invalid MCP toolset runtime config payload");
    }
    const mcpServerName = recordStringField(value, "mcp_server_name") ?? recordStringField(value, "mcpServerName");
    if (mcpServerName === undefined) {
      throw new Error("invalid MCP toolset runtime config payload");
    }
    toolsets.set(mcpServerName, {
      mcpServerName,
      defaultConfig: parseMcpDefaultConfig(recordField(value, "default_config") ?? recordField(value, "defaultConfig")),
      configs: parseMcpToolConfigs(recordArrayField(value, "configs") ?? []),
    });
  }
  return toolsets;
}

function applyMcpToolsetConfig(
  manifest: MCPManifest,
  toolsets: ReadonlyMap<string, MCPManifestToolsetConfig> | undefined,
): MCPManifest | undefined {
  const toolset = toolsets?.get(manifest.mcpServerName);
  if (toolset === undefined) {
    return undefined;
  }
  return {
    ...manifest,
    defaultConfig: toolset.defaultConfig,
    configs: toolset.configs ?? [],
  };
}

function parseMcpManifestTools(values: readonly unknown[]): MCPManifest["tools"] {
  const tools: MCPManifest["tools"][number][] = [];
  for (const value of values) {
    if (!isRecord(value)) {
      throw new Error("invalid MCP manifest runtime config payload");
    }
    const extraFields = Object.keys(value).filter((field) => field !== "name" && field !== "description" && field !== "input_schema");
    const inputSchema = recordField(value, "input_schema");
    if (
      extraFields.length > 0 ||
      typeof value.name !== "string" ||
      value.name.length === 0 ||
      typeof value.description !== "string" ||
      !isRecord(inputSchema)
    ) {
      throw new Error("invalid MCP manifest runtime config payload");
    }
    tools.push({ name: value.name, description: value.description, inputSchema });
  }
  return tools;
}

function parseMcpDefaultConfig(value: unknown): MCPManifest["defaultConfig"] {
  if (!isRecord(value)) {
    return undefined;
  }
  const enabled = typeof value.enabled === "boolean" ? value.enabled : undefined;
  const permissionPolicy = parsePermissionPolicy(value.permission_policy ?? value.permissionPolicy);
  return enabled === undefined && permissionPolicy === undefined ? undefined : { enabled, permissionPolicy };
}

function parseMcpToolConfigs(values: readonly unknown[]): readonly MCPManifestToolConfig[] {
  const configs: MCPManifestToolConfig[] = [];
  for (const value of values) {
    if (!isRecord(value) || typeof value.name !== "string") {
      continue;
    }
    const enabled = typeof value.enabled === "boolean" ? value.enabled : undefined;
    const permissionPolicy = parsePermissionPolicy(value.permission_policy ?? value.permissionPolicy);
    if (enabled === undefined && permissionPolicy === undefined) {
      continue;
    }
    configs.push({ name: value.name, enabled, permissionPolicy });
  }
  return configs;
}

function parseToolConfigs(values: readonly unknown[]): readonly ToolConfig[] {
  const configs: ToolConfig[] = [];
  for (const value of values) {
    if (!isRecord(value) || typeof value.name !== "string" || typeof value.enabled !== "boolean") {
      continue;
    }
    const permissionPolicy = parsePermissionPolicy(value.permission_policy ?? value.permissionPolicy);
    configs.push({
      name: value.name,
      enabled: value.enabled,
      ...(permissionPolicy !== undefined ? { permissionPolicy } : {}),
    });
  }
  return configs;
}

function parsePermissionPolicy(value: unknown): ToolPermissionPolicy | undefined {
  if (value === "always_allow" || value === "always_ask") {
    return value;
  }
  if (isRecord(value)) {
    return parsePermissionPolicy(value.type);
  }
  return undefined;
}

function recordStringField(value: unknown, field: string): string | undefined {
  const child = recordField(value, field);
  return typeof child === "string" && child.length > 0 ? child : undefined;
}

function recordNumberField(value: unknown, field: string): number | undefined {
  const child = recordField(value, field);
  return typeof child === "number" ? child : undefined;
}

function recordField(value: unknown, field: string): unknown {
  return isRecord(value) ? value[field] : undefined;
}

function recordArrayField(value: unknown, field: string): readonly unknown[] | undefined {
  const child = recordField(value, field);
  return Array.isArray(child) ? child : undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value) && Object.getPrototypeOf(value) === Object.prototype;
}

if (import.meta.main) {
  await runRuntimePodCommand();
}
