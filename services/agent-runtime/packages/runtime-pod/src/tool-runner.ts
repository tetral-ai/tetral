/**
 * Routes Runtime Core tool requests to the process or in-process boundary that
 * owns each tool kind. The runner preserves the accepted thread scope and
 * binding on outbound calls, attaches service-account metadata, propagates
 * cancellation to gRPC calls, canonicalizes inputs used for idempotency, and
 * exposes only normalized tool outcomes to Runtime Core. Durable Bridge ACKs
 * precede dependent hot-state changes. A parent/task-name queue serializes child
 * lookup, sends, and lifecycle controls; after resolution a child-id lock guards
 * delivery and lifecycle mutation. Model order sorts entries that are still
 * pending, while an already-running first arrival is not preempted.
 *
 * `buildRuntimePodCommandDependencies` constructs this module and installs its
 * general tool callback plus the separate Sandbox acceptance and result-wait
 * callbacks into ThreadLoop. The runner calls Agent Runtime Bridge for durable
 * sandbox handoff, memory, command, event, and child-thread operations;
 * Provider Gateway for web tools; MCP Connector for MCP tools; and
 * `RuntimeSubAgentRunHost` for local child-thread execution.
 */
import { createHash } from "node:crypto";
import { credentials, status } from "@grpc/grpc-js";
import type { CallOptions, ClientUnaryCall, Metadata, ServiceError } from "@grpc/grpc-js";
import { bridgeAttachmentGrpcChannelOptions, grpcClientChannelOptions, webGrpcChannelOptions } from "./bounds.js";
import {
  AgentRuntimeBridgeServiceClient,
  BridgeWriteStatus,
  ChildControlAction,
  ChildInterruptOutcome,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type {
  AdmitChildInterruptRequest,
  AdmitChildInterruptResponse,
  AwaitChildInterruptRequest,
  AwaitChildInterruptResponse,
  CreateChildThreadRequest,
  CreateChildThreadResponse,
  ListChildThreadsRequest,
  ListChildThreadsResponse,
  MarkChildThreadActiveRequest,
  MarkChildThreadActiveResponse,
  MarkChildThreadClosedRequest,
  MarkChildThreadClosedResponse,
  ReadCommandResultRequest,
  ReadCommandResultResponse,
  ResolveChildThreadRequest,
  ResolveChildThreadResponse,
  ResolveInterAgentDeliveryRequest,
  ResolveInterAgentDeliveryResponse,
  RunMemoryRequest,
  RunMemoryResponse,
  AcceptSandboxExecutionResponse,
  AwaitSandboxExecutionRequest,
  AwaitSandboxExecutionResponse,
  AcceptSandboxExecutionRequest,
  RuntimeScope,
  SendCommandInputRequest,
  SendCommandInputResponse,
  TransientAttachmentRef,
  WriteEventRequest,
  WriteEventResponse,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import {
  McpErrorKind,
  McpRetryStatus,
  McpConnectorServiceClient,
  ProviderGatewayServiceClient,
  RunMcpToolStatus,
  RunWebStatus,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type {
  RunMcpToolRequest,
  RunMcpToolResponse,
  RunWebRequest,
  RunWebResponse,
  McpAttachmentRef,
  ProviderRequestAttachment,
  WebToolInput,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import {
  RuntimeBoundedTextSchema,
  RuntimeFailureSchema,
  SessionEventWriterRetryPolicy,
} from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import { RuntimeMessageSchema } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import type { RuntimeBoundedText, RuntimeJsonValue, RuntimeMessage } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import type {
  RuntimeSandboxExecutionAcceptanceResult,
  RuntimeSandboxExecutionRequest,
  RuntimeToolExecutionRequest,
  RuntimeToolExecutionResult,
} from "@tetral/agent-runtime-core/src/thread-loop/tool-execution.js";
import { selectRecentUserLedTurns } from "@tetral/agent-runtime-core/src/runtime/conversation-turns.js";
import type { RuntimeAcceptedInputState, RuntimeThreadControlState, RuntimeThreadStatusState } from "@tetral/agent-runtime-core/src/thread-loop/thread-state.js";
import { buildOutboundBearerMetadata } from "./auth.js";
import type { ServiceAccountTokenConfig } from "./auth.js";
import type { RuntimeSubAgentRunHost } from "./core-hosts.js";
import { canonicalRunToolJSON } from "@tetral/gateway-protocol/src/run-tool-canonical-json.js";
import { MaxIdBytes, MaxTextBytes } from "@tetral/gateway-protocol/src/bounds.js";
import { validateChildLifecycleDeclarationResponse } from "./bridge-client.js";

// Durable tool-route transport retries rejoin one accepted identity. This
// delay paces only transport uncertainty; it is not an execution-attempt
// budget and never authorizes the provider operation a second time.
const DURABLE_TOOL_REJOIN_DELAY_MS = 300;
// WEB_SEARCH_REQUESTS_MAX / WEB_FETCH_REQUESTS_MAX bound the per-call
// server_tool_use counters accepted from a RunWebResponse usage block before that
// block is attached to the durable web tool result (webServerToolUse below). They
// mirror the web-connector's own per-call request clamps, so a malformed or
// over-counting usage block is rejected here too rather than inflating
// sessions.usage. Derivation: 32 = 8 operations x 4 search domains (a web search
// fans out at most one backend call per honored domain per operation); 8 = 8
// operations x 1 (a web fetch issues one reader call per operation, no domain
// fan-out). A count above the bound drops the whole usage block (webServerToolUse
// returns undefined) so nothing is attached.
// UPDATE-WITH: webServerToolUse (usage-block validation); services/web-connector/
//   types.go (maxOperations=8, maxSearchDomains=4 — the caps these mirror).
const WEB_SEARCH_REQUESTS_MAX = 32;
const WEB_FETCH_REQUESTS_MAX = 8;
// UPDATE-WITH: services/web-connector/types.go; services/gateway/packages/
// provider-gateway/src/bounds.ts.
const WEB_OPERATIONS_MAX = 8;
const WEB_SEARCH_DOMAINS_MAX = 4;
const WEB_DOMAIN_MAX_BYTES = 253;
const TOOL_RESULT_BOUND_FAILURE = "Tool result exceeds the 512 KiB model-visible output limit.";

class ToolResultContractError extends Error {
  constructor() {
    super(TOOL_RESULT_BOUND_FAILURE);
    this.name = "ToolResultContractError";
  }
}

/**
 * Supplies the service endpoints, current accepted-input scope, and injectable
 * boundary adapters used by {@link RuntimePodToolRunner}.
 *
 * Production construction provides addresses and a service-account token path.
 * Tests may replace clients, metadata creation, and cancellation-aware sleep;
 * `subAgentRunHost` is late-bound because Runtime Core creates that host after
 * the runner is assembled.
 */
export interface RuntimePodToolRunnerOptions {
  readonly bridgeAddress: string;
  readonly webAddress: string;
  readonly mcpConnectorAddress: string;
  readonly tokenPath: string;
  readonly subAgentRunHost?: () => RuntimeSubAgentRunHost | undefined;
  readonly metadataFactory?: (config: ServiceAccountTokenConfig) => Promise<Metadata>;
  readonly sleep?: (delayMs: number, abortSignal: AbortSignal) => Promise<void>;
  readonly bridgeClient?: Pick<AgentRuntimeBridgeServiceClient,
    | "acceptSandboxExecution"
    | "awaitSandboxExecution"
    | "runMemory"
    | "sendCommandInput"
    | "readCommandResult"
    | "createChildThread"
    | "resolveChildThread"
    | "listChildThreads"
    | "resolveInterAgentDelivery"
    | "admitChildInterrupt"
    | "awaitChildInterrupt"
    | "markChildThreadClosed"
    | "markChildThreadActive"
    | "writeEvent"
  >;
  readonly webClient?: Pick<ProviderGatewayServiceClient, "runWeb">;
  readonly mcpConnectorClient?: Pick<McpConnectorServiceClient, "runMcpTool">;
}

interface ChildTaskOperationQueueEntry {
  readonly modelOrder: number;
  readonly sequence: number;
  readonly abortSignal: AbortSignal;
  readonly operation: () => Promise<unknown>;
  readonly resolve: (value: unknown) => void;
  readonly reject: (reason: unknown) => void;
  started: boolean;
  removeAbortListener: () => void;
}

interface ChildTaskOperationQueueState {
  running: boolean;
  readonly entries: ChildTaskOperationQueueEntry[];
}

/**
 * Implements the ThreadLoop tool callback across Bridge, Gateway Pod, and
 * in-process child-thread boundaries.
 *
 * Each instance owns its outbound clients and the ephemeral ordering state for
 * same-task child-agent sends and lifecycle controls. Durable idempotency
 * remains owned by the receiving services and is addressed with stable tool,
 * request, and delivery identities.
 */
export class RuntimePodToolRunner {
  private readonly bridgeClient: Pick<AgentRuntimeBridgeServiceClient,
    | "acceptSandboxExecution"
    | "awaitSandboxExecution"
    | "runMemory"
    | "sendCommandInput"
    | "readCommandResult"
    | "createChildThread"
    | "resolveChildThread"
    | "listChildThreads"
    | "resolveInterAgentDelivery"
    | "admitChildInterrupt"
    | "awaitChildInterrupt"
    | "markChildThreadClosed"
    | "markChildThreadActive"
    | "writeEvent"
  >;
  private readonly webClient: Pick<ProviderGatewayServiceClient, "runWeb">;
  private readonly mcpConnectorClient: Pick<McpConnectorServiceClient, "runMcpTool">;
  private readonly metadataFactory: (config: ServiceAccountTokenConfig) => Promise<Metadata>;
  private readonly sleep: (delayMs: number, abortSignal: AbortSignal) => Promise<void>;
  private readonly childOperationLocks = new Map<string, Promise<void>>();
  private readonly childTaskOperationQueues = new Map<string, ChildTaskOperationQueueState>();
  private nextChildTaskOperationSequence = 0;

  /**
   * Creates a runner with injected adapters when present and dedicated gRPC
   * clients for the remaining Bridge, web-connector, and MCP boundaries.
   */
  constructor(private readonly options: RuntimePodToolRunnerOptions) {
    this.bridgeClient =
      options.bridgeClient ?? new AgentRuntimeBridgeServiceClient(options.bridgeAddress, credentials.createInsecure(), bridgeAttachmentGrpcChannelOptions());
    this.webClient =
      options.webClient ?? new ProviderGatewayServiceClient(options.webAddress, credentials.createInsecure(), webGrpcChannelOptions());
    this.mcpConnectorClient =
      options.mcpConnectorClient ?? new McpConnectorServiceClient(options.mcpConnectorAddress, credentials.createInsecure(), grpcClientChannelOptions());
    this.metadataFactory = options.metadataFactory ?? buildOutboundBearerMetadata;
    this.sleep = options.sleep ?? sleepWithAbort;
  }

  /**
   * Executes one ThreadLoop tool request through the route declared by its tool
   * entry and returns the normalized completed, error, or cancelled outcome.
   *
   * Sandbox and memory routes call Bridge, web and MCP routes call Gateway Pod
   * services, and sub-agent routes coordinate durable Bridge state with the
   * local child-thread host.
   */
  async runTool(request: RuntimeToolExecutionRequest): Promise<RuntimeToolExecutionResult> {
    switch (request.entry.route.kind) {
      case "sandbox":
        if (request.entry.route.helperSubcommand === "stdin") {
          return await this.runCommandInput(request);
        }
        return await this.runSandboxTool(request);
      case "bridge":
        return await this.runBridgeTool(request);
      case "gateway":
        return await this.runGatewayTool(request);
      case "subagent":
        return await this.runSubAgentTool(request);
    }
  }

  private async runSandboxTool(request: RuntimeToolExecutionRequest): Promise<RuntimeToolExecutionResult> {
    const acceptance = await this.acceptSandboxExecution(request);
    if (acceptance.type !== "accepted") {
      return acceptance;
    }
    return await this.awaitSandboxExecution(request);
  }

  /** Durably registers one Sandbox execution before any result wait begins. */
  async acceptSandboxExecution(request: RuntimeSandboxExecutionRequest): Promise<RuntimeSandboxExecutionAcceptanceResult> {
    const scope = this.scope(request);
    const inputJson = stableJsonStringify(request.input);
    const durableRequest: AcceptSandboxExecutionRequest = {
      scope,
      toolUseEventId: request.toolUseEventId,
      modelToolCallId: request.modelToolCallId,
      normalizedInputHash: normalizedInputHash(inputJson),
      toolName: request.entry.name,
      inputJson,
    };
    for (;;) {
      try {
        const response = await acceptSandboxExecution(this.bridgeClient, durableRequest, await this.metadata());
        if (!bridgeAckAccepted(response.ack?.status)) {
          return toolFailure(request, "Bridge rejected the sandbox tool call.", true);
        }
        return { type: "accepted" };
      } catch (error) {
        if (isDurableBridgeRejection(error)) {
          return toolFailure(request, "Bridge rejected the sandbox tool call.", false);
        }
        await this.sleep(DURABLE_TOOL_REJOIN_DELAY_MS, new AbortController().signal);
      }
    }
  }

  /** Waits for one already-accepted Sandbox execution without re-registering it. */
  async awaitSandboxExecution(request: RuntimeToolExecutionRequest): Promise<RuntimeToolExecutionResult> {
    const scope = this.scope(request);
    const inputJson = stableJsonStringify(request.input);
    const durableRequest: AwaitSandboxExecutionRequest = {
      scope,
      toolUseEventId: request.toolUseEventId,
      modelToolCallId: request.modelToolCallId,
      normalizedInputHash: normalizedInputHash(inputJson),
      toolName: request.entry.name,
      inputJson,
    };
    for (;;) {
      try {
        const response = await awaitSandboxExecution(this.bridgeClient, durableRequest, await this.metadata(), request.abortSignal);
        if (request.entry.route.kind === "sandbox" && mediaAttachmentHelper(request.entry.route.helperSubcommand)) {
          return withSandboxResultDigest(
            await this.mediaResultToAttachment(request, response.resultJson, response.resultDigest),
            response.resultDigest,
          );
        }
        return withSandboxResultDigest(
          resultJsonToExecutionResult(request, withBackgroundTask(response.resultJson, response.taskId), response.resultDigest),
          response.resultDigest,
        );
      } catch (error) {
        if (isToolRouteAborted(error) || request.abortSignal.aborted) {
          return toolCancelled(request, "Sandbox tool execution was cancelled.");
        }
        if (error instanceof ToolResultContractError) {
          return toolFailure(request, TOOL_RESULT_BOUND_FAILURE, false);
        }
        if (isDurableBridgeRejection(error)) {
          return { type: "stale_custody" };
        }
        await this.sleep(DURABLE_TOOL_REJOIN_DELAY_MS, request.abortSignal);
      }
    }
  }

  private async mediaResultToAttachment(
    request: RuntimeToolExecutionRequest,
    resultJson: string,
    resultDigest: string,
  ): Promise<RuntimeToolExecutionResult> {
    const parsed = parseResultJson(resultJson);
    if (!isRecord(parsed) || stringField(parsed, "status") !== "success") {
      return resultJsonToExecutionResult(request, resultJson, resultDigest);
    }
    const result = recordField(parsed, "result");
    const source = isRecord(result) ? result : parsed;
    const mime = stringField(source, "mime");
    const attachmentRef = stringField(source, "attachment_ref");
    if (attachmentRef !== undefined) {
      if (mime === undefined) {
        return toolFailure(request, `${request.entry.name} returned malformed media attachment metadata.`, false);
      }
      const sourcePath = stringField(source, "source_path") ?? stringField(request.input, "path") ?? stringField(request.input, "file_path") ?? "image";
      const filename = stringField(source, "filename") ?? filenameFromPath(sourcePath);
      const pageRange = stringField(source, "page_range") ?? "";
      const sizeBytes = numberField(source, "size_bytes");
      const lines = [
        "status: success",
        `mime: ${mime}`,
        ...(sizeBytes !== undefined ? [`size_bytes: ${sizeBytes}`] : []),
        ...(pageRange.length > 0 ? [`page_range: ${pageRange}`] : []),
        `attachment: ${filename}`,
      ];
      return {
        type: "completed",
        output: capturedToolText(lines.join("\n")),
        sandboxResultDigest: resultDigest,
        attachments: [providerAttachmentFromBridge({
          attachmentRef,
          mime,
          filename,
          sourceToolUseEventId: stringField(source, "source_tool_use_event_id") ?? request.toolUseEventId,
          sourcePath,
          pageRange,
          detail: stringField(source, "detail") ?? "auto",
        })],
      };
    }
    if (stringField(source, "data_base64") !== undefined) {
      return toolFailure(request, `${request.entry.name} returned raw media payload after Bridge attachment boundary.`, true);
    }
    if (mime === undefined) {
      if (request.entry.route.kind === "sandbox" && request.entry.route.helperSubcommand === "read") {
        return resultJsonToExecutionResult(request, resultJson, resultDigest);
      }
      return toolFailure(request, `${request.entry.name} returned malformed media payload.`, false);
    }
    return toolFailure(request, `${request.entry.name} returned media metadata without an attachment ref.`, true);
  }

  private async runCommandInput(request: RuntimeToolExecutionRequest): Promise<RuntimeToolExecutionResult> {
    const scope = this.scope(request);
    const taskId = taskIdFromInput(request.input);
    if (taskId === undefined) {
      return toolFailure(request, "write_stdin requires a session_id task handle.", false);
    }
    const chars = stringField(request.input, "chars");
    const maxOutputTokens = positiveIntegerField(request.input, "max_output_tokens") ?? 0;
    const commandScope = {
      ...scope,
      requestId: stableId("req", `command-followup:${request.toolUseEventId}`),
    };
    for (;;) {
      try {
        const metadata = await this.metadata();
        if (chars === undefined || chars.length === 0) {
          const response = await readCommandResult(this.bridgeClient, {
            scope: commandScope,
            taskId,
            maxOutputTokens,
            toolUseEventId: request.toolUseEventId,
          }, metadata, request.abortSignal);
          if (!bridgeAckAccepted(response.ack?.status)) {
            return toolFailure(request, "Bridge rejected the command poll.", true);
          }
          return resultJsonToExecutionResult(request, response.resultJson);
        }
        const response = await sendCommandInput(this.bridgeClient, {
          scope: commandScope,
          taskId,
          maxOutputTokens,
          inputJson: stableJsonStringify(request.input),
          toolUseEventId: request.toolUseEventId,
        }, metadata, request.abortSignal);
        if (!bridgeAckAccepted(response.ack?.status)) {
          return toolFailure(request, "Bridge rejected the command input.", true);
        }
        return resultJsonToExecutionResult(request, response.resultJson);
      } catch (error) {
        if (isToolRouteAborted(error) || request.abortSignal.aborted) {
          return toolCancelled(request, "Command task was cancelled.");
        }
        if (isDurableBridgeRejection(error)) {
          return toolFailure(request, "Bridge rejected the command operation.", false);
        }
        await this.sleep(DURABLE_TOOL_REJOIN_DELAY_MS, request.abortSignal);
      }
    }
  }

  private async runBridgeTool(request: RuntimeToolExecutionRequest): Promise<RuntimeToolExecutionResult> {
    if (request.entry.route.operation !== "RunMemory") {
      return toolFailure(request, `Bridge tool route ${request.entry.route.operation} is not installed.`, false);
    }
    const scope = this.scope(request);
    const inputJson = stableJsonStringify(request.input);
    const runMemoryRequest: RunMemoryRequest = {
      scope,
      toolUseEventId: request.toolUseEventId,
      normalizedInputHash: normalizedInputHash(inputJson),
      operation: stringField(request.input, "action") ?? "",
      inputJson,
    };
    while (true) {
      try {
        throwIfToolRouteAborted(request.abortSignal);
        const metadata = await this.metadata();
        throwIfToolRouteAborted(request.abortSignal);
        const response = await runMemory(this.bridgeClient, runMemoryRequest, metadata, request.abortSignal);
        if (!bridgeAckAccepted(response.ack?.status)) {
          return toolFailure(request, "Bridge rejected the memory tool call.", true);
        }
        return resultJsonToExecutionResult(request, response.resultJson);
      } catch (error) {
        if (isToolRouteAborted(error) || request.abortSignal.aborted) {
          return toolCancelled(request, "Memory tool execution was cancelled.");
        }
        if (isDurableBridgeRejection(error)) {
          return toolFailure(request, "Bridge rejected the memory tool call.", false);
        }
        await this.sleep(DURABLE_TOOL_REJOIN_DELAY_MS, request.abortSignal);
      }
    }
  }

  private async runGatewayTool(request: RuntimeToolExecutionRequest): Promise<RuntimeToolExecutionResult> {
    switch (request.entry.route.operation) {
      case "RunWeb":
        return await this.runWebTool(request);
      case "RunMcpTool":
        return await this.runMcpTool(request, request.entry.route.mcpServerName);
    }
    return toolFailure(request, `Gateway tool route ${request.entry.route.operation} is not installed.`, false);
  }

  private async runWebTool(request: RuntimeToolExecutionRequest): Promise<RuntimeToolExecutionResult> {
    const validatedInput = webInputFromRuntime(request.input);
    if (!validatedInput.ok) {
      return toolFailure(request, validatedInput.reason, false);
    }
    try {
      const response = await runWeb(this.webClient, {
        workspaceId: request.workspaceId,
        sessionId: request.sessionId,
        sessionThreadId: request.sessionThreadId,
        toolUseEventId: request.toolUseEventId,
        bindingId: request.bindingId,
        bindingGeneration: request.bindingGeneration,
        runtimeBindingToken: request.runtimeBindingToken,
        input: validatedInput.input,
      }, await this.metadata(), request.abortSignal);
      const serverToolUse = webServerToolUse(response);
      if (serverToolUse === undefined) {
        return toolFailure(request, "Gateway web execution returned malformed usage.", true);
      }
      if (response.status === RunWebStatus.RUN_WEB_STATUS_COMPLETED) {
        return { type: "completed", output: capturedToolText(response.resultText), serverToolUse };
      }
      return toolFailure(
        request,
        response.resultText || "Gateway web execution failed.",
        response.status === RunWebStatus.RUN_WEB_STATUS_RUNTIME_ERROR,
        undefined,
        undefined,
        serverToolUse,
      );
    } catch (error) {
      if (isToolRouteAborted(error) || request.abortSignal.aborted) {
        return toolCancelled(request, "Gateway web execution was cancelled.");
      }
      if (error instanceof ToolResultContractError) {
        return toolFailure(request, TOOL_RESULT_BOUND_FAILURE, false);
      }
      return toolFailure(request, "Gateway web execution is unavailable.", true);
    }
  }

  private async runMcpTool(request: RuntimeToolExecutionRequest, mcpServerName: string): Promise<RuntimeToolExecutionResult> {
    if (mcpServerName.length === 0) {
      return toolFailure(request, "MCP tool route is missing mcp_server_name.", false);
    }
    const scope = this.scope(request);
    const inputJson = stableJsonStringify(request.input);
    try {
      const response = await runMcpTool(this.mcpConnectorClient, {
        requestId: scope.requestId,
        workspaceId: request.workspaceId,
        sessionId: request.sessionId,
        sessionThreadId: request.sessionThreadId,
        toolUseEventId: request.toolUseEventId,
        mcpServerName,
        toolName: request.entry.name,
        inputJson,
        bindingId: request.bindingId,
        bindingGeneration: request.bindingGeneration,
        runtimeBindingToken: request.runtimeBindingToken,
      }, await this.metadata(), request.abortSignal);
      if (response.errorKind === McpErrorKind.MCP_ERROR_KIND_CUSTODY_LOST) {
        return { type: "stale_custody" };
      }
      const materializationHandle = response.materializationHandle;
      if (materializationHandle === undefined || materializationHandle.length === 0) {
        return toolFailure(
          request,
          modelVisibleMcpRuntimeFailure(response.errorKind),
          false,
        );
      }
      if (response.status === RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED) {
        const attachments = response.attachments.map((attachment) => providerAttachmentFromMcp(request, mcpServerName, attachment));
        const completed = completedText(response.resultText);
        if (completed.type !== "completed") {
          return completed;
        }
        return {
          ...completed,
          ...(attachments.length > 0 ? { attachments } : {}),
          mcpMaterializationHandle: materializationHandle,
        };
      }
      const attachments = response.status === RunMcpToolStatus.RUN_MCP_TOOL_STATUS_TOOL_ERROR
        ? response.attachments.map((attachment) => providerAttachmentFromMcp(request, mcpServerName, attachment))
        : [];
      const toolScoped = response.status === RunMcpToolStatus.RUN_MCP_TOOL_STATUS_TOOL_ERROR;
      const message = toolScoped && response.resultText.length > 0
        ? response.resultText
        : modelVisibleMcpRuntimeFailure(response.errorKind);
      return toolFailure(
        request,
        message,
        false,
        undefined,
        attachments,
        undefined,
        materializationHandle,
      );
    } catch (error) {
      if (isToolRouteAborted(error) || request.abortSignal.aborted) {
        return toolCancelled(request, "MCP tool execution was cancelled.");
      }
      if (error instanceof ToolResultContractError) {
        return toolFailure(request, TOOL_RESULT_BOUND_FAILURE, false);
      }
      return toolFailure(request, "The MCP tool outcome could not be confirmed. Check the external service before retrying.", false);
    }
  }

  private async runSubAgentTool(request: RuntimeToolExecutionRequest): Promise<RuntimeToolExecutionResult> {
    switch (request.entry.route.operation) {
      case "spawn_agent":
        return await this.spawnAgent(request);
      case "send_message":
        return await this.sendMessage(request);
      case "wait_agent":
        return await this.waitAgent(request);
      case "interrupt_agent":
        return await this.interruptAgent(request);
      case "close_agent":
        return await this.closeAgent(request);
      case "resume_agent":
        return await this.resumeAgent(request);
      case "list_agents":
        return await this.listAgents(request);
    }
    return toolFailure(request, `Sub-agent tool route ${request.entry.route.operation} is not installed.`, false);
  }

  private async spawnAgent(request: RuntimeToolExecutionRequest): Promise<RuntimeToolExecutionResult> {
    const taskName = requiredString(request.input, "task_name");
    const prompt = requiredString(request.input, "prompt");
    if (taskName === undefined || prompt === undefined) {
      return toolFailure(request, "spawn_agent requires task_name and prompt.", false);
    }
    const agentType = subAgentType(request.input);
    if (agentType === undefined) {
      return toolFailure(request, "spawn_agent agent_type must be general, research, or worker.", false);
    }
    const forkTurns = forkTurnsValue(request.input);
    if (forkTurns === undefined) {
      return toolFailure(request, "spawn_agent fork_turns must be none, all, or a positive integer string.", false);
    }
    const currentModel = request.currentModel;
    if (currentModel === undefined) {
      return toolFailure(request, "spawn_agent requires an inherited current model.", true);
    }
    const parentScope = this.scope(request);
    const host = this.options.subAgentRunHost?.();
    if (host === undefined) {
      return toolFailure(request, "Sub-agent runtime host is unavailable.", true);
    }
    const childThreadId = stableId("thr", `subagent:${request.toolUseEventId}`);
    const childMessage = runtimeUserMessage({
      sessionId: request.sessionId,
      messageId: stableId("msg", `subagent-message:${request.toolUseEventId}:0`),
      text: prompt,
    });
    const threadContextPrefixJson = JSON.stringify({
      source_parent_thread_id: request.sessionThreadId,
      parent_boundary_event_id: request.toolUseEventId,
      source_tool_use_event_id: request.toolUseEventId,
      fork_turns: forkTurns,
      runtime_messages_snapshot: forkedMessages(request.committedMessages, forkTurns),
    });
    try {
      const metadata = await this.metadata();
      const createResponse = await createChildThread(this.bridgeClient, {
        scope: parentScope,
        parentThreadId: request.sessionThreadId,
        childThreadId,
        role: "subagent",
        taskName,
        metadataJson: "{}",
        agentType,
        sourceToolUseEventId: request.toolUseEventId,
        forkTurns,
        threadContextPrefixJson,
        isTrunk: false,
        reviewerReviewId: "",
      }, metadata, request.abortSignal);
      if (!bridgeAckAccepted(createResponse.ack?.status)) {
        return toolFailure(request, createResponse.ack?.errorCode || "Bridge rejected sub-agent creation.", false);
      }
      throwIfToolRouteAborted(request.abortSignal);
      const preloaded = await preloadChildThread(host, request, parentScope, childThreadId, {
        parentThreadId: request.sessionThreadId,
        role: "subagent",
        visibility: "public",
        taskName,
        agentType,
        status: "idle",
      });
      if (!preloaded.ok && preloaded.reason !== "thread_busy") {
        return toolFailure(request, `Sub-agent context preload failed: ${preloaded.reason}.`, preloaded.reason === "local_session_capacity_exceeded");
      }
      const delivery = deliveryIdentity(request.toolUseEventId, childThreadId, 0);
      const sent = await writeThreadMessageSent(this.bridgeClient, parentScope, request, delivery, taskName, childThreadId, childMessage, metadata, request.abortSignal);
      if (!bridgeAckAccepted(sent.ack?.status)) {
        return toolFailure(request, sent.ack?.errorCode || "Bridge rejected sub-agent message send.", true);
      }
      const resolved = await resolveInterAgentDelivery(this.bridgeClient, {
        scope: parentScope,
        childThreadId,
        deliveryId: delivery.deliveryId,
      }, metadata);
      if (
        !bridgeAckAccepted(resolved.ack?.status) ||
        resolved.deliveryId !== delivery.deliveryId ||
        resolved.sourceThreadId !== request.sessionThreadId ||
        resolved.targetThreadId !== childThreadId ||
        resolved.sourceToolUseEventId !== delivery.sourceToolUseEventId ||
        resolved.receivedEventId.length === 0 ||
        resolved.receivedSequence <= 0 ||
        resolved.messageJson.length === 0
      ) {
        return toolFailure(request, resolved.ack?.errorCode || "Bridge could not resolve sub-agent delivery.", true);
      }
      return completedText(`task_name: ${taskName}\nsession_thread_id: ${createResponse.childThreadId || childThreadId}\nstatus: delivered`);
    } catch (error) {
      if (isToolRouteAborted(error) || request.abortSignal.aborted) {
        return toolCancelled(request, "Sub-agent spawn was cancelled.");
      }
      if (error instanceof ToolResultContractError) {
        return toolFailure(request, TOOL_RESULT_BOUND_FAILURE, false);
      }
      if (isGrpcStatus(error, status.ALREADY_EXISTS)) {
        return toolFailure(request, `Sub-agent task_name ${taskName} already exists under this parent thread.`, false);
      }
      return toolFailure(request, "Sub-agent spawn route is unavailable.", true);
    }
  }

  private async sendMessage(request: RuntimeToolExecutionRequest): Promise<RuntimeToolExecutionResult> {
    const taskName = requiredString(request.input, "task_name");
    const messageText = requiredString(request.input, "message");
    if (taskName === undefined || messageText === undefined) {
      return toolFailure(request, "send_message requires task_name and message.", false);
    }
    const currentModel = request.currentModel;
    if (currentModel === undefined) {
      return toolFailure(request, "send_message requires an inherited current model.", true);
    }
    const parentScope = this.scope(request);
    const host = this.options.subAgentRunHost?.();
    if (host === undefined) {
      return toolFailure(request, "Sub-agent runtime host is unavailable.", true);
    }
    try {
      return await this.withChildTaskOperationQueue(request, taskName, async () => {
        const metadata = await this.metadata();
        const child = await this.resolveChildByTaskName(request, parentScope, taskName, metadata);
        if (child === undefined) {
          return toolFailure(request, `No sub-agent named ${taskName} exists under this thread.`, false);
        }
        return await this.withChildOperationLock(request.sessionId, child.sessionThreadId, request.abortSignal, async () => {
          const currentChild = await this.resolveChildById(parentScope, child.sessionThreadId, metadata, request.abortSignal) ?? child;
          if (!childReceivable(currentChild)) {
            return toolFailure(request, `Sub-agent ${taskName} is not receivable in status ${currentChild.status}.`, false);
          }
          throwIfToolRouteAborted(request.abortSignal);
          const preloaded = await preloadChildThread(host, request, parentScope, currentChild.sessionThreadId, {
            parentThreadId: request.sessionThreadId,
            role: "subagent",
            visibility: "public",
            taskName: currentChild.taskName,
            agentType: currentChild.agentType,
            status: currentChild.status,
          });
          if (!preloaded.ok && preloaded.reason !== "thread_busy") {
            return toolFailure(request, `Sub-agent context preload failed: ${preloaded.reason}.`, preloaded.reason === "local_session_capacity_exceeded");
          }
          const childMessage = runtimeUserMessage({
            sessionId: request.sessionId,
            messageId: stableId("msg", `subagent-message:${request.toolUseEventId}:0`),
            text: messageText,
          });
          const delivery = deliveryIdentity(request.toolUseEventId, currentChild.sessionThreadId, 0);
          const sent = await writeThreadMessageSent(this.bridgeClient, parentScope, request, delivery, taskName, currentChild.sessionThreadId, childMessage, metadata, request.abortSignal);
          if (!bridgeAckAccepted(sent.ack?.status)) {
            return toolFailure(request, sent.ack?.errorCode || "Bridge rejected sub-agent message send.", true);
          }
          const resolved = await resolveInterAgentDelivery(this.bridgeClient, {
            scope: parentScope,
            childThreadId: currentChild.sessionThreadId,
            deliveryId: delivery.deliveryId,
          }, metadata);
          if (
            !bridgeAckAccepted(resolved.ack?.status) ||
            resolved.deliveryId !== delivery.deliveryId ||
            resolved.sourceThreadId !== request.sessionThreadId ||
            resolved.targetThreadId !== currentChild.sessionThreadId ||
            resolved.sourceToolUseEventId !== delivery.sourceToolUseEventId ||
            resolved.receivedEventId.length === 0 ||
            resolved.receivedSequence <= 0 ||
            resolved.messageJson.length === 0
          ) {
            return toolFailure(request, resolved.ack?.errorCode || "Bridge could not resolve sub-agent delivery.", true);
          }
          return completedText(`task_name: ${taskName}\nsession_thread_id: ${currentChild.sessionThreadId}\nstatus: delivered`);
        });
      });
    } catch (error) {
      if (isToolRouteAborted(error) || request.abortSignal.aborted) {
        return toolCancelled(request, "Sub-agent send was cancelled.");
      }
      if (error instanceof ToolResultContractError) {
        return toolFailure(request, TOOL_RESULT_BOUND_FAILURE, false);
      }
      return toolFailure(request, "Sub-agent send route is unavailable.", true);
    }
  }

  private async waitAgent(request: RuntimeToolExecutionRequest): Promise<RuntimeToolExecutionResult> {
    const taskName = requiredString(request.input, "task_name");
    if (taskName === undefined) {
      return toolFailure(request, "wait_agent requires task_name.", false);
    }
    const parentScope = this.scope(request);
    try {
      const child = await this.resolveChildByTaskName(request, parentScope, taskName, await this.metadata());
      if (child === undefined) {
        return toolFailure(request, `No sub-agent named ${taskName} exists under this thread.`, false);
      }
      const timeoutMs = numberField(recordInput(request.input), "timeout_ms");
      const host = this.options.subAgentRunHost?.();
      const hotWait = await host?.waitThread(
        threadControlFromRequest(request, parentScope, child.sessionThreadId),
        timeoutMs,
        request.abortSignal,
      );
      const timedOut = hotWait !== undefined && hotWait.ok && hotWait.observed && hotWait.timedOut;
      const settled =
        hotWait !== undefined &&
        hotWait.ok &&
        !timedOut &&
        (hotWait.observed
          ? hotWait.status !== undefined && settledSubAgentStatus(hotWait.status)
          : settledSubAgentStatus(child.status));
      const pulled = settled
        ? await host?.pullAgentMail?.(
            threadControlFromRequest(request, parentScope, request.sessionThreadId),
            child.sessionThreadId,
          )
        : undefined;
      return completedText([
        `task_name: ${taskName}`,
        `session_thread_id: ${child.sessionThreadId}`,
        `status: ${hotWait !== undefined && hotWait.ok ? hotWait.status ?? child.status : child.status}`,
        `timed_out: ${timedOut}`,
        ...(pulled === undefined ? [] : [`final_message:\n${pulled.finalMessage}`]),
      ].join("\n"));
    } catch (error) {
      if (isToolRouteAborted(error) || request.abortSignal.aborted) {
        return toolCancelled(request, "Sub-agent wait was cancelled.");
      }
      if (error instanceof ToolResultContractError) {
        return toolFailure(request, TOOL_RESULT_BOUND_FAILURE, false);
      }
      return toolFailure(request, "Sub-agent wait route is unavailable.", true);
    }
  }

  private async interruptAgent(request: RuntimeToolExecutionRequest): Promise<RuntimeToolExecutionResult> {
    const result = await this.withResolvedChild(request, "interrupt_agent", async (parentScope, child, metadata) => {
      return await this.withChildOperationLock(request.sessionId, child.sessionThreadId, request.abortSignal, async () => {
        throwIfToolRouteAborted(request.abortSignal);
        const control = await this.admitAndAwaitChildInterrupt(
          request,
          parentScope,
          child.sessionThreadId,
          ChildControlAction.CHILD_CONTROL_ACTION_INTERRUPT,
          false,
          metadata,
        );
        if (!control.ok) {
          return toolFailure(request, control.message, control.retryable);
        }
        const rootOutcome = control.response.outcomes.find((entry) =>
          entry.target?.childThreadId === child.sessionThreadId
        )?.outcome;
        const interrupted = rootOutcome === ChildInterruptOutcome.CHILD_INTERRUPT_OUTCOME_COMPLETED ||
          rootOutcome === ChildInterruptOutcome.CHILD_INTERRUPT_OUTCOME_DUPLICATE;
        const terminalStatus = rootOutcome === ChildInterruptOutcome.CHILD_INTERRUPT_OUTCOME_PRESERVED_FAILED
          ? "failed"
          : rootOutcome === ChildInterruptOutcome.CHILD_INTERRUPT_OUTCOME_PRESERVED_TERMINATED
            ? "terminated"
            : undefined;
        return completedText([
          `task_name: ${child.taskName}`,
          `session_thread_id: ${child.sessionThreadId}`,
          `interrupted: ${interrupted}`,
          ...(terminalStatus === undefined ? [] : [`status: ${terminalStatus}`]),
        ].join("\n"));
      });
    });
    return result;
  }

  private async closeAgent(request: RuntimeToolExecutionRequest): Promise<RuntimeToolExecutionResult> {
    return await this.withResolvedChild(request, "close_agent", async (parentScope, child, metadata) => {
      const host = this.options.subAgentRunHost?.();
      if (host === undefined) {
        return toolFailure(request, "Sub-agent runtime host is unavailable.", true);
      }
      return await this.withChildOperationLock(request.sessionId, child.sessionThreadId, request.abortSignal, async () => {
        const control = await this.admitAndAwaitChildInterrupt(
          request,
          parentScope,
          child.sessionThreadId,
          ChildControlAction.CHILD_CONTROL_ACTION_CLOSE,
          true,
          metadata,
        );
        if (!control.ok) {
          return toolFailure(request, control.message, control.retryable);
        }
        const response = await markChildThreadClosed(this.bridgeClient, {
          scope: parentScope,
          childThreadId: child.sessionThreadId,
          source: { sourceToolUseEventId: request.toolUseEventId },
          sourceToolUseEventId: request.toolUseEventId,
          targets: control.targets,
        }, metadata, request.abortSignal);
        const declaration = validateChildLifecycleDeclarationResponse({
          action: "close",
          sessionThreadId: request.sessionThreadId,
          childThreadId: child.sessionThreadId,
          sourceKind: "tool_use",
          sourceCommandId: request.toolUseEventId,
          bindingId: request.bindingId,
          bindingGeneration: request.bindingGeneration,
        }, response);
        if (!declaration.ok) {
          if (declaration.discardHotState) {
            return { type: "stale_custody" };
          }
          return toolFailure(request, declaration.errorCode, true);
        }
        const rootDisposition = declaration.dispositions.find(
          (stamp) => stamp.childThreadId === child.sessionThreadId,
        )?.disposition;
        let rootRunExitOutcome: string | undefined;
        for (const stamp of declaration.dispositions) {
          const lifecycle = await host.markThreadClosed(
            threadControlFromRequest(request, parentScope, stamp.childThreadId),
          );
          if (!lifecycle.ok) {
            return toolFailure(request, `Sub-agent close was not accepted: ${lifecycle.reason}.`, true);
          }
          if (stamp.childThreadId === child.sessionThreadId) {
            rootRunExitOutcome = lifecycle.runExitOutcome;
          }
        }
        const rootStatus = rootDisposition === "preserved_failed"
          ? "failed"
          : rootDisposition === "preserved_terminated"
            ? "terminated"
            : "closed_for_runtime";
        return completedText([
          `task_name: ${child.taskName}`,
          `session_thread_id: ${child.sessionThreadId}`,
          `status: ${rootStatus}`,
          ...(rootRunExitOutcome === undefined ? [] : [`run_outcome: ${rootRunExitOutcome}`]),
        ].join("\n"));
      });
    });
  }

  private async admitAndAwaitChildInterrupt(
    request: RuntimeToolExecutionRequest,
    parentScope: RuntimeScope,
    rootChildThreadId: string,
    action: ChildControlAction,
    includeDescendants: boolean,
    metadata: Metadata,
  ): Promise<
    | { readonly ok: true; readonly targets: AdmitChildInterruptResponse["targets"]; readonly response: AwaitChildInterruptResponse }
    | { readonly ok: false; readonly retryable: boolean; readonly message: string }
  > {
    let admitted: AdmitChildInterruptResponse;
    try {
      admitted = await admitChildInterrupt(this.bridgeClient, {
        scope: parentScope,
        rootChildThreadId,
        sourceToolUseEventId: request.toolUseEventId,
        action,
        includeDescendants,
      }, metadata, request.abortSignal);
    } catch (error) {
      return { ok: false, retryable: !isDurableBridgeRejection(error), message: "Sub-agent interrupt admission failed." };
    }
    if (!bridgeAckAccepted(admitted.ack?.status) || admitted.targets.length === 0) {
      return { ok: false, retryable: false, message: admitted.ack?.errorCode || "Sub-agent interrupt admission was rejected." };
    }
    const awaitRequest: AwaitChildInterruptRequest = {
      scope: parentScope,
      rootChildThreadId,
      sourceToolUseEventId: request.toolUseEventId,
      action,
      includeDescendants,
      targets: admitted.targets,
    };
    while (true) {
      throwIfToolRouteAborted(request.abortSignal);
      try {
        const response = await awaitChildInterrupt(this.bridgeClient, awaitRequest, metadata, request.abortSignal);
        if (!bridgeAckAccepted(response.ack?.status) || response.outcomes.length !== admitted.targets.length) {
          return { ok: false, retryable: false, message: response.ack?.errorCode || "Sub-agent interrupt completion was malformed." };
        }
        const failed = response.outcomes.find((entry) =>
          entry.outcome === ChildInterruptOutcome.CHILD_INTERRUPT_OUTCOME_DELIVERY_FAILED
        );
        if (failed !== undefined) {
          return { ok: false, retryable: false, message: failed.errorCode ?? "Sub-agent interrupt delivery failed." };
        }
        return { ok: true, targets: admitted.targets, response };
      } catch (error) {
        if (!isGrpcStatus(error, status.DEADLINE_EXCEEDED)) {
          return { ok: false, retryable: !isDurableBridgeRejection(error), message: "Sub-agent interrupt completion is unavailable." };
        }
        await this.sleep(DURABLE_TOOL_REJOIN_DELAY_MS, request.abortSignal);
      }
    }
  }

  // A durable terminal receipt completes without installing hot state; reopened children require hot residency.
  private async resumeAgent(request: RuntimeToolExecutionRequest): Promise<RuntimeToolExecutionResult> {
    return await this.withResolvedChild(request, "resume_agent", async (parentScope, child, metadata) => {
      const host = this.options.subAgentRunHost?.();
      if (host === undefined) {
        return toolFailure(request, "Sub-agent runtime host is unavailable.", true);
      }
      return await this.withChildOperationLock(request.sessionId, child.sessionThreadId, request.abortSignal, async () => {
        const control = threadControlFromRequest(request, parentScope, child.sessionThreadId);
		if (child.status === "failed" || child.status === "terminated") {
		  return completedText([
			`task_name: ${child.taskName}`,
			`session_thread_id: ${child.sessionThreadId}`,
			`status: ${child.status}`,
		  ].join("\n"));
		}
		let preloadedClosed = false;
		if (child.status === "closed_for_runtime") {
		  const preloaded = await preloadChildThread(host, request, parentScope, child.sessionThreadId, {
			parentThreadId: request.sessionThreadId,
			role: "subagent",
			visibility: "public",
			taskName: child.taskName,
			agentType: child.agentType,
			status: "closed_for_runtime",
		  });
		  if (!preloaded.ok) {
			return toolFailure(request, `Sub-agent resume context preload failed: ${preloaded.reason}.`, true);
		  }
		  preloadedClosed = true;
		}
		let response: MarkChildThreadActiveResponse;
		try {
		  response = await markChildThreadActive(this.bridgeClient, {
          scope: parentScope,
          childThreadId: child.sessionThreadId,
          source: { sourceToolUseEventId: request.toolUseEventId },
        }, metadata, request.abortSignal);
		} catch (error) {
		  if (preloadedClosed) {
			await host.markThreadClosed(control);
		  }
		  throw error;
		}
        const declaration = validateChildLifecycleDeclarationResponse({
          action: "resume",
          sessionThreadId: request.sessionThreadId,
          childThreadId: child.sessionThreadId,
          sourceKind: "tool_use",
          sourceCommandId: request.toolUseEventId,
          bindingId: request.bindingId,
          bindingGeneration: request.bindingGeneration,
        }, response);
        if (!declaration.ok) {
		  if (preloadedClosed) {
			await host.markThreadClosed(control);
		  }
          if (declaration.discardHotState) {
            return { type: "stale_custody" };
          }
          return toolFailure(request, declaration.errorCode, true);
        }
        const disposition = declaration.dispositions[0]?.disposition;
        // MarkChildThreadActive is the durable resume boundary. A resident
        // quiescent copy follows that receipt, while a notification that has
        // already created a run slot is further ahead and must not be stopped.
        // Missing or otherwise non-applicable hot state is disposable; the
        // next access reconstructs the durable idle Thread.
        if (disposition === "resumed") {
          await host.markThreadActive(control).catch(() => undefined);
        }
        const inspected = await host.inspectThread(control).catch(() => undefined);
        const activeStatus = inspected?.ok === true && inspected.observed
          ? inspected.status ?? "idle"
          : disposition === "resumed" ? "idle" : child.status;
        return completedText(`task_name: ${child.taskName}\nsession_thread_id: ${child.sessionThreadId}\nstatus: ${activeStatus}`);
      });
    });
  }

  private async listAgents(request: RuntimeToolExecutionRequest): Promise<RuntimeToolExecutionResult> {
    const parentScope = this.scope(request);
    try {
      const response = await listChildThreads(this.bridgeClient, {
        scope: parentScope,
        parentThreadId: request.sessionThreadId,
      }, await this.metadata(), request.abortSignal);
      if (!bridgeAckAccepted(response.ack?.status)) {
        return toolFailure(request, response.ack?.errorCode || "Bridge rejected sub-agent list.", true);
      }
      const children = response.threadJson.map(parseChildThread).filter((child): child is ChildThreadRecord => child !== undefined && child.role === "subagent");
      return completedText(JSON.stringify({
        agents: children.map((child) => ({
          task_name: child.taskName,
          session_thread_id: child.sessionThreadId,
          status: child.status,
          agent_type: child.agentType,
        })),
      }, null, 2));
    } catch (error) {
      if (error instanceof ToolResultContractError) {
        return toolFailure(request, TOOL_RESULT_BOUND_FAILURE, false);
      }
      return toolFailure(request, "Sub-agent list route is unavailable.", true);
    }
  }

  private async withResolvedChild(
    request: RuntimeToolExecutionRequest,
    toolName: string,
    action: (parentScope: RuntimeScope, child: ChildThreadRecord, metadata: Metadata) => Promise<RuntimeToolExecutionResult>,
  ): Promise<RuntimeToolExecutionResult> {
    const taskName = requiredString(request.input, "task_name");
    if (taskName === undefined) {
      return toolFailure(request, `${toolName} requires task_name.`, false);
    }
    const parentScope = this.scope(request);
    try {
      return await this.withChildTaskOperationQueue(request, taskName, async () => {
        const metadata = await this.metadata();
        const child = await this.resolveChildByTaskName(request, parentScope, taskName, metadata);
        if (child === undefined) {
          return toolFailure(request, `No sub-agent named ${taskName} exists under this thread.`, false);
        }
        return await action(parentScope, child, metadata);
      });
    } catch (error) {
      if (isToolRouteAborted(error) || request.abortSignal.aborted) {
        return toolCancelled(request, `${toolName} was cancelled.`);
      }
      if (error instanceof ToolResultContractError) {
        return toolFailure(request, TOOL_RESULT_BOUND_FAILURE, false);
      }
      return toolFailure(request, `${toolName} route is unavailable.`, true);
    }
  }

  private async resolveChildByTaskName(
    request: RuntimeToolExecutionRequest,
    parentScope: RuntimeScope,
    taskName: string,
    metadata: Metadata,
  ): Promise<ChildThreadRecord | undefined> {
    const listed = await listChildThreads(this.bridgeClient, {
      scope: parentScope,
      parentThreadId: request.sessionThreadId,
    }, metadata, request.abortSignal);
    if (!bridgeAckAccepted(listed.ack?.status)) {
      return undefined;
    }
    const child = listed.threadJson.map(parseChildThread)
      .find((candidate): candidate is ChildThreadRecord =>
        candidate !== undefined &&
        candidate.role === "subagent" &&
        candidate.taskName === taskName
      );
    if (child === undefined) {
      return undefined;
    }
    const resolved = await resolveChildThread(this.bridgeClient, {
      scope: parentScope,
      childThreadId: child.sessionThreadId,
    }, metadata, request.abortSignal);
    if (!bridgeAckAccepted(resolved.ack?.status)) {
      return undefined;
    }
    return parseChildThread(resolved.threadJson) ?? child;
  }

  private async resolveChildById(
    parentScope: RuntimeScope,
    childThreadId: string,
    metadata: Metadata,
    abortSignal: AbortSignal,
  ): Promise<ChildThreadRecord | undefined> {
    const resolved = await resolveChildThread(this.bridgeClient, {
      scope: parentScope,
      childThreadId,
    }, metadata, abortSignal);
    if (!bridgeAckAccepted(resolved.ack?.status)) {
      return undefined;
    }
    return parseChildThread(resolved.threadJson);
  }

  private async withChildTaskOperationQueue<T>(
    request: RuntimeToolExecutionRequest,
    taskName: string,
    operation: () => Promise<T>,
  ): Promise<T> {
    const key = `${request.sessionId}\x1f${request.sessionThreadId}\x1ftask:${taskName}`;
    return await this.withOrderedChildTaskOperation(key, request.modelOrder, request.abortSignal, operation);
  }

  private async withOrderedChildTaskOperation<T>(
    key: string,
    modelOrder: number,
    abortSignal: AbortSignal,
    operation: () => Promise<T>,
  ): Promise<T> {
    return await new Promise<T>((resolve, reject) => {
      if (abortSignal.aborted) {
        reject(new ToolRouteAborted());
        return;
      }
      let queue = this.childTaskOperationQueues.get(key);
      if (queue === undefined) {
        queue = { running: false, entries: [] };
        this.childTaskOperationQueues.set(key, queue);
      }
      const entry: ChildTaskOperationQueueEntry = {
        modelOrder,
        sequence: this.nextChildTaskOperationSequence,
        abortSignal,
        operation,
        resolve: (value) => resolve(value as T),
        reject,
        started: false,
        removeAbortListener: () => undefined,
      };
      const abort = (): void => {
        if (entry.started) {
          return;
        }
        const currentQueue = this.childTaskOperationQueues.get(key);
        const index = currentQueue?.entries.indexOf(entry) ?? -1;
        if (currentQueue === undefined || index < 0) {
          return;
        }
        currentQueue.entries.splice(index, 1);
        entry.removeAbortListener();
        reject(new ToolRouteAborted());
        if (!currentQueue.running && currentQueue.entries.length === 0) {
          this.childTaskOperationQueues.delete(key);
        }
      };
      abortSignal.addEventListener("abort", abort, { once: true });
      entry.removeAbortListener = () => abortSignal.removeEventListener("abort", abort);
      queue.entries.push(entry);
      this.nextChildTaskOperationSequence += 1;
      this.pumpChildTaskOperationQueue(key);
    });
  }

  private pumpChildTaskOperationQueue(key: string): void {
    const queue = this.childTaskOperationQueues.get(key);
    if (queue === undefined || queue.running) {
      return;
    }
    queue.entries.sort(compareChildTaskOperationEntries);
    const entry = queue.entries.shift();
    if (entry === undefined) {
      this.childTaskOperationQueues.delete(key);
      return;
    }
    queue.running = true;
    entry.started = true;
    void Promise.resolve()
      .then(() => {
        throwIfToolRouteAborted(entry.abortSignal);
        return entry.operation();
      })
      .then(entry.resolve, entry.reject)
      .finally(() => {
        entry.removeAbortListener();
        const currentQueue = this.childTaskOperationQueues.get(key);
        if (currentQueue === undefined) {
          return;
        }
        currentQueue.running = false;
        if (currentQueue.entries.length === 0) {
          this.childTaskOperationQueues.delete(key);
          return;
        }
        this.pumpChildTaskOperationQueue(key);
      });
  }

  private async withChildOperationLock<T>(
    sessionId: string,
    childThreadId: string,
    abortSignal: AbortSignal,
    operation: () => Promise<T>,
  ): Promise<T> {
    const key = `${sessionId}\x1f${childThreadId}`;
    const previous = this.childOperationLocks.get(key) ?? Promise.resolve();
    let releaseGate = (): void => undefined;
    const gate = new Promise<void>((resolve) => {
      releaseGate = resolve;
    });
    const next = previous.catch(() => undefined).then(() => gate);
    this.childOperationLocks.set(key, next);
    try {
      await waitForPromiseOrAbort(previous.catch(() => undefined), abortSignal);
    } catch (error) {
      releaseGate();
      void next.finally(() => {
        if (this.childOperationLocks.get(key) === next) {
          this.childOperationLocks.delete(key);
        }
      });
      throw error;
    }
    try {
      throwIfToolRouteAborted(abortSignal);
      return await operation();
    } finally {
      releaseGate();
      if (this.childOperationLocks.get(key) === next) {
        this.childOperationLocks.delete(key);
      }
    }
  }

  private scope(request: RuntimeSandboxExecutionRequest): RuntimeScope {
    return {
      requestId: stableId("req", `tool:${request.modelRequestId}:${request.modelToolCallId}`),
      workspaceId: request.workspaceId,
      sessionId: request.sessionId,
      sessionThreadId: request.sessionThreadId,
      binding: {
        bindingId: request.bindingId,
        bindingGeneration: request.bindingGeneration,
        targetPodUid: request.targetPodUid,
      },
    };
  }

  private async metadata(): Promise<Metadata> {
    return await this.metadataFactory({ tokenPath: this.options.tokenPath });
  }
}

function modelVisibleMcpRuntimeFailure(errorKind: McpErrorKind | undefined): string {
  switch (errorKind) {
    case McpErrorKind.MCP_ERROR_KIND_IN_FLIGHT:
      return "The MCP tool execution is still in progress. Check the external service before retrying.";
    case McpErrorKind.MCP_ERROR_KIND_COMMIT_FAILED:
      return "The MCP tool may have completed, but its result could not be confirmed. Check the external service before retrying.";
    case McpErrorKind.MCP_ERROR_KIND_CLAIM_CONFLICT:
      return "The MCP tool request conflicts with an existing execution. Check the external service before retrying.";
    case McpErrorKind.MCP_ERROR_KIND_INTERNAL:
    case McpErrorKind.MCP_ERROR_KIND_UNSPECIFIED:
    case McpErrorKind.UNRECOGNIZED:
    case undefined:
      return "The MCP tool outcome could not be confirmed. Check the external service before retrying.";
    default:
      return "MCP tool execution is unavailable.";
  }
}

interface ChildThreadRecord {
  readonly sessionThreadId: string;
  readonly parentThreadId?: string | undefined;
  readonly role: string;
  readonly status: RuntimeThreadStatusState;
  readonly taskName?: string | undefined;
  readonly agentType: "general" | "research" | "worker";
}

interface DeliveryIdentity {
  readonly deliveryId: string;
  readonly sourceToolUseEventId: string;
}

function completedText(text: string): RuntimeToolExecutionResult {
  return { type: "completed", output: capturedToolText(text) };
}

function capturedToolText(text: string) {
  const parsed = RuntimeBoundedTextSchema.safeParse({ text, truncated: false });
  if (!parsed.success) {
    throw new ToolResultContractError();
  }
  return parsed.data;
}

function stableId(prefix: string, seed: string): string {
  return `${prefix}_${createHash("sha256").update(seed).digest("hex").slice(0, 32)}`;
}

function deliveryIdentity(sourceToolUseEventId: string, childThreadId: string, deliveryIndex: number): DeliveryIdentity {
  return {
    deliveryId: stableId("delivery", `${sourceToolUseEventId}:${childThreadId}:${deliveryIndex}`),
    sourceToolUseEventId,
  };
}

function compareChildTaskOperationEntries(left: ChildTaskOperationQueueEntry, right: ChildTaskOperationQueueEntry): number {
  if (left.modelOrder !== right.modelOrder) {
    return left.modelOrder - right.modelOrder;
  }
  return left.sequence - right.sequence;
}

async function preloadChildThread(
  host: RuntimeSubAgentRunHost,
  request: RuntimeToolExecutionRequest,
  parentScope: RuntimeScope,
  childThreadId: string,
  thread: Extract<RuntimeAcceptedInputState, { readonly kind: "inter_agent_message" }>["thread"],
): Promise<Awaited<ReturnType<RuntimeSubAgentRunHost["preloadThread"]>>> {
  return await host.preloadThread({
    ...threadControlFromRequest(request, parentScope, childThreadId),
    thread,
  });
}

async function writeThreadMessageSent(
  client: Pick<AgentRuntimeBridgeServiceClient, "writeEvent">,
  parentScope: RuntimeScope,
  request: RuntimeToolExecutionRequest,
  delivery: DeliveryIdentity,
  taskName: string,
  childThreadId: string,
  message: RuntimeMessage,
  metadata: Metadata,
  abortSignal: AbortSignal,
): Promise<WriteEventResponse> {
  return await writeEvent(client, {
    scope: parentScope,
    runtimeWriteId: stableId("rtw", `thread-message-sent:${delivery.deliveryId}`),
    modelRequestId: "",
    eventType: "agent.thread_message_sent",
    payloadJson: JSON.stringify({
      type: "agent.thread_message_sent",
      delivery_id: delivery.deliveryId,
      source_thread_id: request.sessionThreadId,
      target_thread_id: childThreadId,
      target_task_name: taskName,
      source_tool_use_event_id: delivery.sourceToolUseEventId,
      message,
    }),
    sessionVisible: true,
    serverToolUse: undefined,
    contextThroughMessageSequence: undefined,
    requestKind: "",
  }, metadata, abortSignal);
}

function runtimeUserMessage(input: {
  readonly sessionId: string;
  readonly messageId: string;
  readonly text: string;
}): RuntimeMessage {
  const now = new Date().toISOString();
  return RuntimeMessageSchema.parse({
    id: input.messageId,
    sessionId: input.sessionId,
    role: "user",
    origin: "runtime",
    sequence: 0,
    status: "completed",
    createdAt: now,
    parts: [
      {
        id: `${input.messageId}_text`,
        sessionId: input.sessionId,
        messageId: input.messageId,
        sequence: 0,
        type: "text",
        text: input.text,
        truncated: false,
        status: "completed",
        createdAt: now,
        completedAt: now,
      },
    ],
  });
}

function forkedMessages(messages: readonly RuntimeMessage[], forkTurns: string): readonly RuntimeMessage[] {
  if (forkTurns === "none") {
    return [];
  }
  if (forkTurns === "all") {
    return messages;
  }
  const count = Number.parseInt(forkTurns, 10);
  return selectRecentUserLedTurns(messages, count);
}

function requiredString(input: RuntimeJsonValue, field: string): string | undefined {
  const value = stringField(input, field);
  return value !== undefined && value.trim().length > 0 ? value.trim() : undefined;
}

function subAgentType(input: RuntimeJsonValue): "general" | "research" | "worker" | undefined {
  const value = stringField(input, "agent_type") ?? "general";
  return value === "general" || value === "research" || value === "worker" ? value : undefined;
}

function forkTurnsValue(input: RuntimeJsonValue): string | undefined {
  const value = stringField(input, "fork_turns") ?? "all";
  if (value === "none" || value === "all") {
    return value;
  }
  if (value.length === 0 || value[0] === "0") {
    return undefined;
  }
  return [...value].every((char) => char >= "0" && char <= "9") ? value : undefined;
}

function parseChildThread(threadJson: string): ChildThreadRecord | undefined {
  try {
    const parsed = JSON.parse(threadJson) as unknown;
    if (!isRecord(parsed)) {
      return undefined;
    }
    const sessionThreadId = stringField(parsed, "session_thread_id");
    const role = stringField(parsed, "role");
    const statusValue = stringField(parsed, "status");
    if (sessionThreadId === undefined || role === undefined || statusValue === undefined) {
      return undefined;
    }
    if (!isThreadStatus(statusValue)) {
      return undefined;
    }
    const rawAgentType = stringField(parsed, "agent_type") ?? "general";
    const agentType = rawAgentType === "research" || rawAgentType === "worker" ? rawAgentType : "general";
    return {
      sessionThreadId,
      ...(stringField(parsed, "parent_thread_id") !== undefined ? { parentThreadId: stringField(parsed, "parent_thread_id") } : {}),
      role,
      status: statusValue,
      ...(stringField(parsed, "task_name") !== undefined ? { taskName: stringField(parsed, "task_name") } : {}),
      agentType,
    };
  } catch {
    return undefined;
  }
}

function childReceivable(child: ChildThreadRecord): boolean {
  return child.status !== "closed_for_runtime" && child.status !== "terminated" && child.status !== "failed";
}

function settledSubAgentStatus(statusValue: RuntimeThreadStatusState): boolean {
  return statusValue === "idle" ||
    statusValue === "failed" ||
    statusValue === "terminated" ||
    statusValue === "closed_for_runtime";
}

function isThreadStatus(value: string): value is RuntimeThreadStatusState {
  return value === "idle" ||
    value === "running" ||
    value === "requires_action" ||
    value === "closed_for_runtime" ||
    value === "rescheduling" ||
    value === "terminated" ||
    value === "failed";
}

function threadControlFromRequest(
  request: RuntimeToolExecutionRequest,
  parentScope: RuntimeScope,
  childThreadId: string,
): RuntimeThreadControlState {
  return {
    requestId: stableId("req", `thread-control:${request.toolUseEventId}:${childThreadId}`),
    workspaceId: parentScope.workspaceId,
    sessionId: parentScope.sessionId,
    sessionThreadId: childThreadId,
    bindingId: parentScope.binding?.bindingId ?? request.bindingId,
    bindingGeneration: parentScope.binding?.bindingGeneration ?? request.bindingGeneration,
    targetPodUid: parentScope.binding?.targetPodUid ?? "",
    runtimeInputId: stableId("rin", `thread-control:${request.toolUseEventId}:${childThreadId}`),
    eventIds: [],
    sequenceFrom: 0,
    sequenceTo: 0,
  };
}

function recordInput(input: RuntimeJsonValue): Record<string, RuntimeJsonValue> {
  return isRecord(input) ? input : {};
}

function acceptSandboxExecution(
  client: Pick<AgentRuntimeBridgeServiceClient, "acceptSandboxExecution">,
  request: AcceptSandboxExecutionRequest,
  metadata: Metadata,
): Promise<AcceptSandboxExecutionResponse> {
  const options: CallOptions = {
    deadline: Date.now() + SessionEventWriterRetryPolicy.timeoutPerAttemptMs,
  };
  return new Promise((resolve, reject) => {
    try {
      client.acceptSandboxExecution(request, metadata, options, (error, response) => {
        if (error !== null) {
          reject(error);
          return;
        }
        resolve(response);
      });
    } catch (error) {
      reject(error);
    }
  });
}

function awaitSandboxExecution(
  client: Pick<AgentRuntimeBridgeServiceClient, "awaitSandboxExecution">,
  request: AwaitSandboxExecutionRequest,
  metadata: Metadata,
  abortSignal: AbortSignal,
): Promise<AwaitSandboxExecutionResponse> {
  return cancellableUnaryCall(request, metadata, abortSignal, (unaryRequest, unaryMetadata, callback) =>
    client.awaitSandboxExecution(unaryRequest, unaryMetadata, callback)
  );
}

function runMemory(
  client: Pick<AgentRuntimeBridgeServiceClient, "runMemory">,
  request: RunMemoryRequest,
  metadata: Metadata,
  abortSignal: AbortSignal,
): Promise<RunMemoryResponse> {
  return cancellableUnaryCall(request, metadata, abortSignal, (unaryRequest, unaryMetadata, callback) =>
    client.runMemory(unaryRequest, unaryMetadata, callback)
  );
}

function sendCommandInput(
  client: Pick<AgentRuntimeBridgeServiceClient, "sendCommandInput">,
  request: SendCommandInputRequest,
  metadata: Metadata,
  abortSignal: AbortSignal,
): Promise<SendCommandInputResponse> {
  return cancellableUnaryCall(request, metadata, abortSignal, (unaryRequest, unaryMetadata, callback) =>
    client.sendCommandInput(unaryRequest, unaryMetadata, callback)
  );
}

function readCommandResult(
  client: Pick<AgentRuntimeBridgeServiceClient, "readCommandResult">,
  request: ReadCommandResultRequest,
  metadata: Metadata,
  abortSignal: AbortSignal,
): Promise<ReadCommandResultResponse> {
  return cancellableUnaryCall(request, metadata, abortSignal, (unaryRequest, unaryMetadata, callback) =>
    client.readCommandResult(unaryRequest, unaryMetadata, callback)
  );
}

function createChildThread(
  client: Pick<AgentRuntimeBridgeServiceClient, "createChildThread">,
  request: CreateChildThreadRequest,
  metadata: Metadata,
  abortSignal: AbortSignal,
): Promise<CreateChildThreadResponse> {
  return cancellableUnaryCall(request, metadata, abortSignal, (unaryRequest, unaryMetadata, callback) =>
    client.createChildThread(unaryRequest, unaryMetadata, callback)
  );
}

function resolveChildThread(
  client: Pick<AgentRuntimeBridgeServiceClient, "resolveChildThread">,
  request: ResolveChildThreadRequest,
  metadata: Metadata,
  abortSignal: AbortSignal,
): Promise<ResolveChildThreadResponse> {
  return cancellableUnaryCall(request, metadata, abortSignal, (unaryRequest, unaryMetadata, callback) =>
    client.resolveChildThread(unaryRequest, unaryMetadata, callback)
  );
}

function listChildThreads(
  client: Pick<AgentRuntimeBridgeServiceClient, "listChildThreads">,
  request: ListChildThreadsRequest,
  metadata: Metadata,
  abortSignal: AbortSignal,
): Promise<ListChildThreadsResponse> {
  return cancellableUnaryCall(request, metadata, abortSignal, (unaryRequest, unaryMetadata, callback) =>
    client.listChildThreads(unaryRequest, unaryMetadata, callback)
  );
}

function resolveInterAgentDelivery(
  client: Pick<AgentRuntimeBridgeServiceClient, "resolveInterAgentDelivery">,
  request: ResolveInterAgentDeliveryRequest,
  metadata: Metadata,
): Promise<ResolveInterAgentDeliveryResponse> {
  return unaryCall(request, metadata, (unaryRequest, unaryMetadata, callback) =>
    client.resolveInterAgentDelivery(unaryRequest, unaryMetadata, callback)
  );
}

function admitChildInterrupt(
  client: Pick<AgentRuntimeBridgeServiceClient, "admitChildInterrupt">,
  request: AdmitChildInterruptRequest,
  metadata: Metadata,
  abortSignal: AbortSignal,
): Promise<AdmitChildInterruptResponse> {
  return cancellableUnaryCall(request, metadata, abortSignal, (unaryRequest, unaryMetadata, callback) =>
    client.admitChildInterrupt(unaryRequest, unaryMetadata, callback)
  );
}

function awaitChildInterrupt(
  client: Pick<AgentRuntimeBridgeServiceClient, "awaitChildInterrupt">,
  request: AwaitChildInterruptRequest,
  metadata: Metadata,
  abortSignal: AbortSignal,
): Promise<AwaitChildInterruptResponse> {
  return cancellableUnaryCall(request, metadata, abortSignal, (unaryRequest, unaryMetadata, callback) =>
    client.awaitChildInterrupt(unaryRequest, unaryMetadata, callback)
  );
}

function markChildThreadClosed(
  client: Pick<AgentRuntimeBridgeServiceClient, "markChildThreadClosed">,
  request: MarkChildThreadClosedRequest,
  metadata: Metadata,
  abortSignal: AbortSignal,
): Promise<MarkChildThreadClosedResponse> {
  return cancellableUnaryCall(request, metadata, abortSignal, (unaryRequest, unaryMetadata, callback) =>
    client.markChildThreadClosed(unaryRequest, unaryMetadata, callback)
  );
}

function markChildThreadActive(
  client: Pick<AgentRuntimeBridgeServiceClient, "markChildThreadActive">,
  request: MarkChildThreadActiveRequest,
  metadata: Metadata,
  abortSignal: AbortSignal,
): Promise<MarkChildThreadActiveResponse> {
  return cancellableUnaryCall(request, metadata, abortSignal, (unaryRequest, unaryMetadata, callback) =>
    client.markChildThreadActive(unaryRequest, unaryMetadata, callback)
  );
}

function writeEvent(
  client: Pick<AgentRuntimeBridgeServiceClient, "writeEvent">,
  request: WriteEventRequest,
  metadata: Metadata,
  abortSignal: AbortSignal,
): Promise<WriteEventResponse> {
  return cancellableUnaryCall(request, metadata, abortSignal, (unaryRequest, unaryMetadata, callback) =>
    client.writeEvent(unaryRequest, unaryMetadata, callback)
  );
}

function runWeb(
  client: Pick<ProviderGatewayServiceClient, "runWeb">,
  request: RunWebRequest,
  metadata: Metadata,
  abortSignal: AbortSignal,
): Promise<RunWebResponse> {
  return cancellableUnaryCall(request, metadata, abortSignal, (unaryRequest, unaryMetadata, callback) =>
    client.runWeb(unaryRequest, unaryMetadata, callback)
  );
}

function runMcpTool(
  client: Pick<McpConnectorServiceClient, "runMcpTool">,
  request: RunMcpToolRequest,
  metadata: Metadata,
  abortSignal: AbortSignal,
): Promise<RunMcpToolResponse> {
  return cancellableUnaryCall(
    request,
    metadata,
    abortSignal,
    (unaryRequest, unaryMetadata, callback) =>
      client.runMcpTool(unaryRequest, unaryMetadata, callback)
  );
}

type UnaryCallback<Response> = (error: ServiceError | null, response: Response) => void;
type UnaryInvoker<Request, Response> = (
  request: Request,
  metadata: Metadata,
  callback: UnaryCallback<Response>,
) => ClientUnaryCall;

function unaryCall<Request, Response>(
  request: Request,
  metadata: Metadata,
  invoke: UnaryInvoker<Request, Response>,
): Promise<Response> {
  return new Promise((resolve, reject) => {
    try {
      invoke(request, metadata, (error: ServiceError | null, response: Response) => {
        if (error !== null) {
          reject(error);
          return;
        }
        resolve(response);
      });
    } catch (error) {
      reject(error);
    }
  });
}

class ToolRouteAborted extends Error {
  constructor() {
    super("tool route aborted");
  }
}

function throwIfToolRouteAborted(abortSignal: AbortSignal): void {
  if (abortSignal.aborted) {
    throw new ToolRouteAborted();
  }
}

function sleepWithAbort(delayMs: number, abortSignal: AbortSignal): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    if (abortSignal.aborted) {
      reject(new ToolRouteAborted());
      return;
    }
    let timer: ReturnType<typeof setTimeout> | undefined;
    let settled = false;
    const cleanup = (): void => {
      if (timer !== undefined) {
        clearTimeout(timer);
        timer = undefined;
      }
      abortSignal.removeEventListener("abort", abort);
    };
    const settle = (settlement: () => void): void => {
      if (settled) {
        return;
      }
      settled = true;
      cleanup();
      settlement();
    };
    const abort = (): void => {
      settle(() => reject(new ToolRouteAborted()));
    };
    abortSignal.addEventListener("abort", abort, { once: true });
    timer = setTimeout(() => settle(resolve), delayMs);
  });
}

function waitForPromiseOrAbort<T>(promise: Promise<T>, abortSignal: AbortSignal): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    if (abortSignal.aborted) {
      reject(new ToolRouteAborted());
      return;
    }
    const abort = (): void => {
      reject(new ToolRouteAborted());
    };
    abortSignal.addEventListener("abort", abort, { once: true });
    promise.then(
      (value) => {
        abortSignal.removeEventListener("abort", abort);
        resolve(value);
      },
      (error) => {
        abortSignal.removeEventListener("abort", abort);
        reject(error);
      },
    );
  });
}

function cancellableUnaryCall<Request, Response>(
  request: Request,
  metadata: Metadata,
  abortSignal: AbortSignal,
  invoke: UnaryInvoker<Request, Response>,
): Promise<Response> {
  return new Promise((resolve, reject) => {
    if (abortSignal.aborted) {
      reject(new ToolRouteAborted());
      return;
    }
    let settled = false;
    let call: ClientUnaryCall | undefined;
    const cleanup = (): void => {
      abortSignal.removeEventListener("abort", abort);
    };
    const settle = (settlement: () => void): void => {
      if (settled) {
        return;
      }
      settled = true;
      cleanup();
      settlement();
    };
    const abort = (): void => {
      call?.cancel();
      settle(() => reject(new ToolRouteAborted()));
    };
    abortSignal.addEventListener("abort", abort, { once: true });
    try {
      call = invoke(request, metadata, (error: ServiceError | null, response: Response) => {
        if (error !== null) {
          settle(() => reject(error));
          return;
        }
        settle(() => resolve(response));
      });
    } catch (error) {
      settle(() => reject(error));
    }
  });
}

function isToolRouteAborted(error: unknown): boolean {
  return error instanceof ToolRouteAborted ||
    (typeof error === "object" && error !== null && (error as Partial<ServiceError>).code === status.CANCELLED);
}

function isGrpcStatus(error: unknown, code: status): boolean {
  return typeof error === "object" && error !== null && (error as Partial<ServiceError>).code === code;
}

function isDurableBridgeRejection(error: unknown): boolean {
  if (typeof error !== "object" || error === null || typeof (error as Partial<ServiceError>).code !== "number") {
    return false;
  }
  switch ((error as Partial<ServiceError>).code) {
    case status.INVALID_ARGUMENT:
    case status.NOT_FOUND:
    case status.ALREADY_EXISTS:
    case status.FAILED_PRECONDITION:
      return true;
    default:
      return false;
  }
}

function bridgeAckAccepted(status: BridgeWriteStatus | undefined): boolean {
  return status === BridgeWriteStatus.BRIDGE_WRITE_STATUS_COMMITTED || status === BridgeWriteStatus.BRIDGE_WRITE_STATUS_DUPLICATE;
}

function resultJsonToExecutionResult(
  request: RuntimeToolExecutionRequest,
  resultJson: string,
  sandboxResultDigest?: string,
): RuntimeToolExecutionResult {
  const parsed = parseResultJson(resultJson);
  if (parsed === undefined) {
    return toolFailure(request, "Tool route returned malformed result JSON.", false);
  }
  const visible = filterVisibleToolResult(request, parsed);
  const activationFailure = sandboxActivationExhaustionResult(request, visible, sandboxResultDigest);
  if (activationFailure !== undefined) {
    return activationFailure;
  }
  const status = isRecord(visible) ? stringField(visible, "status") : undefined;
  const retryableValue = isRecord(visible) && Object.hasOwn(visible, "retryable")
    ? visible.retryable
    : undefined;
  if (retryableValue !== undefined && typeof retryableValue !== "boolean") {
    return {
      ...toolFailure(request, "Tool route returned malformed retryability.", false),
      ...(sandboxResultDigest !== undefined ? { sandboxResultDigest } : {}),
    };
  }
  if (status === "success" || status === "completed" || status === "running" || status === "accepted") {
    const backgroundTask = status === "running" ? backgroundTaskFromResult(visible) : undefined;
    let output: RuntimeBoundedText;
    try {
      output = RuntimeBoundedTextSchema.parse({
        ...capturedToolText(formatToolResult(request, visible)),
        truncated: resultIsTruncated(parsed),
      });
    } catch (error) {
      if (error instanceof ToolResultContractError) {
        return {
          ...toolFailure(request, TOOL_RESULT_BOUND_FAILURE, false),
          ...(sandboxResultDigest !== undefined ? { sandboxResultDigest } : {}),
        };
      }
      throw error;
    }
    return {
      type: "completed",
      output,
      ...(backgroundTask !== undefined ? { backgroundTask } : {}),
      ...(sandboxResultDigest !== undefined ? { sandboxResultDigest } : {}),
    };
  }
  if (status === "cancelled" || status === "expired") {
    return {
      type: "cancelled",
      error: runtimeFailure(request, resultErrorMessage(request, visible, `Tool route ${status}.`), false),
      ...(sandboxResultDigest !== undefined ? { sandboxResultDigest } : {}),
    };
  }
  return {
    type: "error",
    error: runtimeFailure(
      request,
      resultErrorMessage(request, visible, "Tool route failed."),
      typeof retryableValue === "boolean" ? retryableValue : status === "runtime_error" || status === "failed",
    ),
    ...(sandboxResultDigest !== undefined ? { sandboxResultDigest } : {}),
  };
}

// Activation exhaustion is a private lifecycle settlement. Every Sandbox Tool
// family converges here before Runtime failure and public Tool Result creation,
// so route envelopes, partial output, and provider diagnosis cannot alter the
// single model-visible failure text.
function sandboxActivationExhaustionResult(
  request: RuntimeToolExecutionRequest,
  parsed: RuntimeJsonValue,
  sandboxResultDigest?: string,
): RuntimeToolExecutionResult | undefined {
  if (request.entry.route.kind !== "sandbox" || !isRecord(parsed)) {
    return undefined;
  }
  const error = recordField(parsed, "error");
  if (!isRecord(error) || stringField(error, "kind") !== "sandbox_activation_attempts_exhausted") {
    return undefined;
  }
  return {
    ...toolFailure(request, "sandbox activation could not be completed", false),
    ...(sandboxResultDigest !== undefined ? { sandboxResultDigest } : {}),
  };
}

function withSandboxResultDigest(
  result: RuntimeToolExecutionResult,
  sandboxResultDigest: string,
): RuntimeToolExecutionResult {
  return result.type === "stale_custody" ? result : { ...result, sandboxResultDigest };
}

function backgroundTaskFromResult(parsed: RuntimeJsonValue): { readonly taskId: string } | undefined {
  if (!isRecord(parsed)) {
    return undefined;
  }
  const result = recordField(parsed, "result");
  const source = isRecord(result) ? result : parsed;
  const taskId = stringField(source, "task_id") ?? stringField(parsed, "task_id");
  if (taskId === undefined || taskId.length === 0) {
    return undefined;
  }
  return { taskId };
}

function parseResultJson(resultJson: string): RuntimeJsonValue | undefined {
  try {
    const parsed = JSON.parse(resultJson.length === 0 ? "{}" : resultJson) as unknown;
    return isRuntimeJsonValue(parsed) ? parsed : undefined;
  } catch {
    return undefined;
  }
}

function withBackgroundTask(resultJson: string, taskId: string): string {
  if (taskId.length === 0) {
    return resultJson;
  }
  const parsed = parseResultJson(resultJson);
  if (!isRecord(parsed) || stringField(parsed, "task_id") !== undefined) {
    return resultJson;
  }
  return stableJsonStringify({ ...parsed, task_id: taskId });
}

function resultErrorMessage(request: RuntimeToolExecutionRequest, parsed: RuntimeJsonValue, fallback: string): string {
  let message = fallback;
  if (isRecord(parsed)) {
    const topLevelMessage = stringField(parsed, "message");
    if (topLevelMessage !== undefined && topLevelMessage.length > 0) {
      message = topLevelMessage;
    }
    const error = parsed.error;
    if (isRecord(error)) {
      const errorMessage = stringField(error, "message");
      if (errorMessage !== undefined && errorMessage.length > 0) {
        message = errorMessage;
      }
      const kind = stringField(error, "kind");
      if (message === fallback && kind !== undefined && kind.length > 0) {
        message = `Tool route failed with ${kind}.`;
      }
    }
    const errorCode = stringField(parsed, "error_code");
    if (message === fallback && errorCode !== undefined && errorCode.length > 0) {
      message = `Tool route failed with ${errorCode}.`;
    }
  }
  const partial = formatToolResult(request, parsed);
  return partial.length > 0 ? `${message}\n\nPartial result:\n${partial}` : message;
}

function formatToolResult(request: RuntimeToolExecutionRequest, parsed: RuntimeJsonValue): string {
  if (isRecord(parsed)) {
    const resultText = stringField(parsed, "result_text") ?? stringField(parsed, "resultText");
    if (resultText !== undefined) {
      return resultText;
    }
    if (request.entry.route.kind === "sandbox" && (request.entry.route.helperSubcommand === "exec" || request.entry.route.helperSubcommand === "stdin")) {
      return formatCommandEnvelope(parsed);
    }
    if (request.entry.route.kind === "bridge" && request.entry.route.operation === "RunMemory") {
      return formatMemoryEnvelope(parsed);
    }
    if (request.entry.route.kind === "sandbox") {
      return formatSandboxEnvelope(parsed, request.entry.route.helperSubcommand === "read");
    }
  }
  return visibleJsonText(parsed);
}

function formatCommandEnvelope(parsed: Record<string, RuntimeJsonValue>): string {
  const result = recordField(parsed, "result");
  const source = isRecord(result) ? result : parsed;
  const lines = [`status: ${stringField(parsed, "status") ?? "unknown"}`];
  const taskId = stringField(source, "task_id") ?? stringField(parsed, "task_id");
  if (taskId !== undefined) {
    lines.push(`session_id: ${taskId}`);
  }
  const exitCode = numberField(source, "exit_code");
  if (exitCode !== undefined) {
    lines.push(`exit_code: ${exitCode}`);
  }
  const signal = stringField(source, "signal");
  if (signal !== undefined) {
    lines.push(`signal: ${signal}`);
  }
  appendStream(lines, "stdout", recordField(source, "stdout"));
  appendStream(lines, "stderr", recordField(source, "stderr"));
  const text = stringField(source, "text");
  if (text !== undefined) {
    lines.push("output:");
    lines.push(text);
  }
  return lines.join("\n");
}

function formatMemoryEnvelope(parsed: Record<string, RuntimeJsonValue>): string {
  const lines = [`status: ${stringField(parsed, "status") ?? "unknown"}`];
  for (const field of ["action", "path", "new_path", "summary", "message"] as const) {
    const value = stringField(parsed, field);
    if (value !== undefined && value.length > 0) {
      lines.push(`${field}: ${value}`);
    }
  }
  for (const field of ["error_code", "reread_required", "projection_refreshed", "retryable"] as const) {
    const value = parsed[field];
    if (typeof value === "string" || typeof value === "boolean") {
      lines.push(`${field}: ${String(value)}`);
    }
  }
  const conflictingPaths = parsed.conflicting_paths;
  if (Array.isArray(conflictingPaths)) {
    lines.push(`conflicting_paths: ${JSON.stringify(conflictingPaths)}`);
  }
  return lines.length > 1 ? lines.join("\n") : visibleJsonText(parsed);
}

function formatSandboxEnvelope(parsed: Record<string, RuntimeJsonValue>, numberReadContent: boolean): string {
  const result = recordField(parsed, "result");
  const source = isRecord(result) ? result : parsed;
  const lines = [`status: ${stringField(parsed, "status") ?? "unknown"}`];
  if (numberReadContent) {
    appendReadContent(lines, source);
  }
  for (const field of ["path", "file_path", "content", "message", "summary", "next_offset", "total_lines"] as const) {
    if (numberReadContent && field === "content") {
      continue;
    }
    const value = source[field];
    if (typeof value === "string" && value.length > 0) {
      lines.push(`${field}: ${value}`);
    } else if (typeof value === "number") {
      lines.push(`${field}: ${value}`);
    }
  }
  const rendered = lines.length > 1 ? lines.join("\n") : visibleJsonText(source);
  const truncationLines: string[] = [];
  if (resultIsTruncated(parsed)) {
    truncationLines.push("truncated: true");
  }
  const lineTruncations = numberField(source, "line_truncations");
  if (lineTruncations !== undefined && lineTruncations > 0) {
    truncationLines.push(`line_truncations: ${lineTruncations}`);
  }
  return truncationLines.length > 0 ? `${rendered}\n${truncationLines.join("\n")}` : rendered;
}

function appendReadContent(lines: string[], source: Record<string, RuntimeJsonValue>): void {
  const content = stringField(source, "content");
  if (content === undefined) {
    return;
  }
  lines.push("content:");
  const withoutTrailingNewline = content.endsWith("\n") ? content.slice(0, -1) : content;
  const returnedLines = numberField(source, "returned_lines") ?? 0;
  let contentLines: string[];
  if (withoutTrailingNewline.length > 0) {
    contentLines = withoutTrailingNewline.split("\n");
  } else if (returnedLines > 0) {
    contentLines = [""];
  } else {
    contentLines = [];
  }
  if (contentLines.length === 0) {
    return;
  }
  const startLine = numberField(source, "start_line");
  if (startLine === undefined) {
    lines.push(...contentLines);
    return;
  }
  const firstLine = BigInt(startLine);
  for (let index = 0; index < contentLines.length; index++) {
    const lineNumber = (firstLine + BigInt(index)).toString().padStart(6, " ");
    lines.push(`${lineNumber}\t${contentLines[index] ?? ""}`);
  }
}

function resultIsTruncated(value: RuntimeJsonValue): boolean {
  if (!isRecord(value)) {
    return false;
  }
  const result = recordField(value, "result");
  return value.truncated === true || (isRecord(result) && result.truncated === true);
}

function appendStream(lines: string[], label: "stdout" | "stderr", value: unknown): void {
  if (!isRecord(value)) {
    return;
  }
  const text = stringField(value, "text");
  if (text === undefined || text.length === 0) {
    return;
  }
  lines.push(`${label}:`);
  lines.push(text);
  if (value.truncated === true) {
    lines.push(`${label}_truncated: true`);
  }
}

function visibleJsonText(value: RuntimeJsonValue): string {
  return JSON.stringify(filterVisibleJson(value), null, 2);
}

function filterVisibleToolResult(request: RuntimeToolExecutionRequest, value: RuntimeJsonValue): RuntimeJsonValue {
  const forbidden = new Set(request.entry.formatter.forbiddenFields.map(normalizedResultField));
  return filterVisibleJson(value, forbidden);
}

function filterVisibleJson(value: RuntimeJsonValue, declaredForbidden: ReadonlySet<string> = new Set()): RuntimeJsonValue {
  if (Array.isArray(value)) {
    return value.map((item) => filterVisibleJson(item, declaredForbidden));
  }
  if (!isRecord(value)) {
    return value;
  }
  return Object.fromEntries(
    Object.entries(value)
      .filter(([key]) => !forbiddenResultField(key) && !declaredForbidden.has(normalizedResultField(key)))
      .map(([key, child]) => [key, filterVisibleJson(child, declaredForbidden)]),
  );
}

function normalizedResultField(key: string): string {
  return key.replace(/[^a-z0-9]/giu, "").toLowerCase();
}

function forbiddenResultField(key: string): boolean {
  const normalized = key.toLowerCase();
  return normalized.includes("payload") ||
    normalized.includes("provider") ||
    normalized.includes("daytona") ||
    normalized.includes("sandbox_process") ||
    normalized.includes("sandbox_driver") ||
    normalized.includes("credential") ||
    normalized === "data_base64" ||
    normalized.includes("base64") ||
    normalized === "binding_id" ||
    normalized === "bindingid" ||
    normalized === "runtimebindingtoken";
}

function webServerToolUse(response: RunWebResponse): { readonly webSearchRequests: number; readonly webFetchRequests: number } | undefined {
  const usage = response.usage;
  if (usage === undefined ||
    !Number.isSafeInteger(usage.webSearchRequests) || usage.webSearchRequests < 0 || usage.webSearchRequests > WEB_SEARCH_REQUESTS_MAX ||
    !Number.isSafeInteger(usage.webFetchRequests) || usage.webFetchRequests < 0 || usage.webFetchRequests > WEB_FETCH_REQUESTS_MAX) {
    return undefined;
  }
  return {
    webSearchRequests: usage.webSearchRequests,
    webFetchRequests: usage.webFetchRequests,
  };
}

function toolFailure(
  request: RuntimeSandboxExecutionRequest,
  message: string,
  retryable: boolean,
  retryStatus?: ReturnType<typeof runtimeRetryStatusFromMcp>,
  attachments?: readonly ProviderRequestAttachment[],
  serverToolUse?: { readonly webSearchRequests: number; readonly webFetchRequests: number },
  mcpMaterializationHandle?: string,
): Extract<RuntimeToolExecutionResult, { readonly type: "error" }> {
  return {
    type: "error",
    error: runtimeFailure(request, message, retryable, retryStatus),
    ...(attachments !== undefined && attachments.length > 0 ? { attachments } : {}),
    ...(serverToolUse !== undefined ? { serverToolUse } : {}),
    ...(mcpMaterializationHandle !== undefined ? { mcpMaterializationHandle } : {}),
  };
}

function toolCancelled(request: RuntimeToolExecutionRequest, message: string): RuntimeToolExecutionResult {
  return {
    type: "cancelled",
    error: runtimeFailure(request, message, false),
  };
}

function runtimeFailure(
  request: RuntimeSandboxExecutionRequest,
  message: string,
  retryable: boolean,
  retryStatus?: ReturnType<typeof runtimeRetryStatusFromMcp>,
) {
  return RuntimeFailureSchema.parse({
    type: "runtime",
    code: "runtime_invalid_sequence",
    message,
    retryable,
    fatal: false,
    sessionId: request.sessionId,
    ...(retryStatus !== undefined ? { retryStatus } : {}),
  });
}

function runtimeRetryStatusFromMcp(retryStatus: McpRetryStatus | undefined) {
  switch (retryStatus) {
    case McpRetryStatus.MCP_RETRY_STATUS_RETRYING:
      return { type: "retrying" as const, attempt: 1 };
    case McpRetryStatus.MCP_RETRY_STATUS_EXHAUSTED:
      return { type: "exhausted" as const };
    case McpRetryStatus.MCP_RETRY_STATUS_TERMINAL:
      return { type: "terminal" as const };
    case McpRetryStatus.MCP_RETRY_STATUS_UNSPECIFIED:
    case undefined:
      return undefined;
    case McpRetryStatus.UNRECOGNIZED:
      return undefined;
  }
}

function providerAttachmentFromBridge(attachment: TransientAttachmentRef): ProviderRequestAttachment {
  return {
    transient: {
      attachmentRef: attachment.attachmentRef,
      sourceToolUseEventId: attachment.sourceToolUseEventId,
      sourcePath: attachment.sourcePath,
      pageRange: attachment.pageRange,
      detail: attachment.detail,
    },
    fileBacked: undefined,
    mime: attachment.mime,
    filename: attachment.filename,
  };
}

function providerAttachmentFromMcp(
  request: RuntimeToolExecutionRequest,
  mcpServerName: string,
  attachment: McpAttachmentRef,
): ProviderRequestAttachment {
  return {
    transient: {
      attachmentRef: attachment.attachmentRef,
      sourceToolUseEventId: request.toolUseEventId,
      sourcePath: `mcp:${mcpServerName}/${attachment.suggestedFilename}`,
      pageRange: "",
      detail: "auto",
    },
    fileBacked: undefined,
    mime: attachment.mime,
    filename: attachment.suggestedFilename,
  };
}

function mediaAttachmentHelper(helperSubcommand: string): boolean {
  return helperSubcommand === "view_image" || helperSubcommand === "read";
}

function filenameFromPath(value: string): string {
  const parts = value.split(/[\\/]/).filter((part) => part.length > 0);
  return parts.at(-1) ?? "image";
}

function normalizedInputHash(inputJson: string): string {
  return createHash("sha256").update(inputJson).digest("hex");
}

function stableJsonStringify(value: RuntimeJsonValue): string {
  return canonicalRunToolJSON(JSON.stringify(value));
}

function taskIdFromInput(input: RuntimeJsonValue): string | undefined {
  return stringField(input, "session_id");
}

function stringField(input: RuntimeJsonValue, field: string): string | undefined {
  return isRecord(input) && typeof input[field] === "string" ? input[field] : undefined;
}

function positiveIntegerField(input: RuntimeJsonValue, field: string): number | undefined {
  const value = isRecord(input) ? input[field] : undefined;
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0 ? value : undefined;
}

function recordField(input: RuntimeJsonValue, field: string): RuntimeJsonValue | undefined {
  return isRecord(input) ? input[field] : undefined;
}

type WebInputValidation =
  | { readonly ok: true; readonly input: WebToolInput }
  | { readonly ok: false; readonly reason: string };

function webInputFromRuntime(input: RuntimeJsonValue): WebInputValidation {
  if (!isRecord(input)) {
    return { ok: false, reason: "web input must be an object." };
  }
  const rawSearchQuery = optionalArrayField(input, "search_query");
  if (rawSearchQuery === undefined) {
    return { ok: false, reason: "web search_query must be an array." };
  }
  const rawOpen = optionalArrayField(input, "open");
  if (rawOpen === undefined) {
    return { ok: false, reason: "web open must be an array." };
  }
  const rawFind = optionalArrayField(input, "find");
  if (rawFind === undefined) {
    return { ok: false, reason: "web find must be an array." };
  }
  const operationCount = rawSearchQuery.length + rawOpen.length + rawFind.length;
  if (operationCount === 0) {
    return { ok: false, reason: "web requires at least one search, open, or find operation." };
  }
  if (operationCount > WEB_OPERATIONS_MAX) {
    return { ok: false, reason: `web accepts at most ${WEB_OPERATIONS_MAX} operations.` };
  }

  const searchQuery: Array<{ readonly q: string; readonly domains: string[] }> = [];
  for (const item of rawSearchQuery) {
    if (!isRecord(item)) {
      return { ok: false, reason: "web search_query contains an invalid operation." };
    }
    const q = stringField(item, "q");
    const rawDomains = optionalArrayField(item, "domains");
    if (
      rawDomains === undefined ||
      q === undefined ||
      invalidUtf8Bytes(q, MaxTextBytes) ||
      rawDomains.length > WEB_SEARCH_DOMAINS_MAX ||
      rawDomains.some((domain) => typeof domain !== "string" || invalidUtf8Bytes(domain, WEB_DOMAIN_MAX_BYTES))
    ) {
      return { ok: false, reason: "web search_query contains an invalid operation." };
    }
    const domains = rawDomains.filter((domain): domain is string => typeof domain === "string");
    searchQuery.push({ q, domains });
  }

  const open: Array<{ readonly url?: string; readonly refId?: string; readonly lineno?: number }> = [];
  for (const item of rawOpen) {
    if (!isRecord(item)) {
      return { ok: false, reason: "web open contains an invalid operation." };
    }
    const url = stringField(item, "url");
    const refId = stringField(item, "ref_id");
    const lineno = numberField(item, "lineno");
    const hasUrl = url !== undefined && url !== "";
    const hasRef = refId !== undefined && refId !== "";
    if (
      hasUrl === hasRef ||
      (url !== undefined && invalidUtf8Bytes(url, MaxTextBytes)) ||
      (refId !== undefined && invalidUtf8Bytes(refId, MaxIdBytes)) ||
      (lineno !== undefined && lineno < 1)
    ) {
      return { ok: false, reason: "web open contains an invalid operation." };
    }
    open.push({
      ...(url !== undefined ? { url } : {}),
      ...(refId !== undefined ? { refId } : {}),
      ...(lineno !== undefined ? { lineno } : {}),
    });
  }

  const find: Array<{ readonly refId: string; readonly pattern: string }> = [];
  for (const item of rawFind) {
    if (!isRecord(item)) {
      return { ok: false, reason: "web find contains an invalid operation." };
    }
    const refId = stringField(item, "ref_id");
    const pattern = stringField(item, "pattern");
    if (
      refId === undefined ||
      pattern === undefined ||
      invalidUtf8Bytes(refId, MaxIdBytes) ||
      encodedBytes(pattern) > MaxTextBytes
    ) {
      return { ok: false, reason: "web find contains an invalid operation." };
    }
    find.push({ refId, pattern });
  }
  return { ok: true, input: { searchQuery, open, find } };
}

function invalidUtf8Bytes(value: string, maxBytes: number): boolean {
  return value.length === 0 || encodedBytes(value) > maxBytes;
}

function encodedBytes(value: string): number {
  return new TextEncoder().encode(value).byteLength;
}

function optionalArrayField(input: Record<string, RuntimeJsonValue>, field: string): readonly RuntimeJsonValue[] | undefined {
  const value = input[field];
  return value === undefined || Array.isArray(value) ? (value ?? []) : undefined;
}

function numberField(input: Record<string, RuntimeJsonValue>, field: string): number | undefined {
  const value = input[field];
  return typeof value === "number" && Number.isSafeInteger(value) ? value : undefined;
}

function isRuntimeJsonValue(value: unknown): value is RuntimeJsonValue {
  if (value === null || typeof value === "boolean" || typeof value === "string") {
    return true;
  }
  if (typeof value === "number") {
    return Number.isFinite(value);
  }
  if (Array.isArray(value)) {
    return value.every(isRuntimeJsonValue);
  }
  if (!isRecord(value)) {
    return false;
  }
  return Object.values(value).every(isRuntimeJsonValue);
}

function isRecord(value: unknown): value is Record<string, RuntimeJsonValue> {
  return typeof value === "object" && value !== null && !Array.isArray(value) && Object.getPrototypeOf(value) === Object.prototype;
}
