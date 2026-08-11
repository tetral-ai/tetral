/**
 * @packageDocumentation
 *
 * Implements the gRPC-facing orchestration shell for MCP tool discovery and
 * execution. It authenticates internal callers, verifies Runtime bindings for
 * tool calls, enforces readiness and catalog admission, coordinates durable
 * claim and commit, and gives every tool call one terminal settlement record.
 * It reports each successfully re-listed manifest to Bridge and retries bounded
 * manifest-change notifications without turning exhaustion into a readiness transition.
 *
 * The gRPC server and MCP list-change callback call this module. It delegates
 * MCP I/O to `McpClient`, durable result ownership to `McpIdempotencyStore`,
 * binding checks to `RuntimeBindingTokenVerifier`, and changed-manifest
 * delivery to `McpManifestChangeNotifier`.
 */
import { Metadata, status } from "@grpc/grpc-js";
import { createHash } from "node:crypto";
import { semanticErrorFields } from "@tetral/ts-observability";
import type { TetralLogRecord } from "@tetral/ts-observability";
import {
  McpErrorKind,
  McpRetryStatus,
  RunMcpToolStatus,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { RuntimeBindingTokenVerifier } from "@tetral/gateway-protocol/src/binding-token.js";
import type {
  ListMcpToolsRequest,
  ListMcpToolsResponse,
  McpToolDefinition,
  RunMcpToolRequest,
  RunMcpToolResponse,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { catalogEntryByName } from "./catalog.js";
import { validateListMcpToolsRequest, validateListMcpToolsResponse, validatePendingRunMcpToolResponse, validateRunMcpToolRequest } from "./bounds.js";
import { McpConnectorError, mcpErrorKind } from "./errors.js";
import { formatMcpToolResult } from "./formatter.js";
import { McpIdempotencyStaleCustodyError, canonicalJson, normalizedInputHash } from "./idempotency.js";
import { McpConnectorMetricsRegistry } from "./metrics.js";
import type { McpConnectorErrorCode } from "./errors.js";
import type { McpCallToolResult } from "./formatter.js";
import type { McpIdempotencyContext, McpIdempotencyStore, PendingRunMcpToolResponse } from "./idempotency.js";
import type { McpConnectorRunToolMetric } from "./metrics.js";

/**
 * Authenticates an internal gRPC request before its readiness, catalog,
 * credential, or MCP work.
 *
 * A successful result supplies the workload identity and pod UID used to bind
 * tool execution to the authenticated Runtime pod.
 */
export interface McpAuthenticator {
  authenticate(input: { readonly metadata: Metadata; readonly method: string }): Promise<{
    readonly ok: true;
    readonly serviceAccount: { readonly namespace: string; readonly name: string; readonly podUid: string };
  } | {
    readonly ok: false;
    readonly code: "Unauthenticated" | "PermissionDenied";
    readonly message: string;
  }>;
}

/**
 * Supplies MCP discovery and execution after the service admits a request.
 *
 * Implementations own MCP connection, credential, and transport-retry behavior;
 * the service shell owns durable call coordination and outward settlement.
 */
export interface McpClient {
  listTools(input: {
    readonly workspaceId: string;
    readonly sessionId: string;
    readonly mcpServerName: string;
  }): Promise<readonly McpClientTool[]>;
  callTool(input: {
    readonly workspaceId: string;
    readonly sessionId: string;
    readonly sessionThreadId: string;
    readonly mcpServerName: string;
    readonly toolName: string;
    readonly input: Record<string, unknown>;
  }): Promise<McpCallToolResult>;
  connectionCount?: (() => number) | undefined;
}

/**
 * Describes one server-discovered MCP tool before platform collision filtering
 * and manifest serialization.
 */
export interface McpClientTool {
  readonly name: string;
  readonly description: string;
  readonly inputSchema: Record<string, unknown>;
  readonly enabled?: boolean | undefined;
}

/**
 * Receives structured manifest lifecycle records and terminal tool-call
 * operation records from the service shell.
 */
export interface McpConnectorLogger {
  readonly info: (record: McpLogRecord) => void;
  readonly error: (record: McpLogRecord) => void;
}

/**
 * Notifies Bridge that a platform-filtered manifest identity changed.
 *
 * The result distinguishes a durable acknowledgement from retryable and
 * terminal notification failures so the service retries only the failures it
 * owns.
 */
export interface McpManifestChangeNotifier {
  notify(input: {
    readonly workspaceId: string;
    readonly sessionId: string;
    readonly mcpServerName: string;
    readonly manifestEtag: string;
  }): Promise<{ readonly ok: true; readonly duplicate: boolean } | { readonly ok: false; readonly retryable: boolean; readonly code: string; readonly message: string }>;
}

/**
 * Defines the waits after failed manifest notifications; together with the
 * initial call these delays produce four total attempts.
 */
export const MCP_MANIFEST_NOTIFY_RETRY_DELAYS_MS = [1_000, 4_000, 16_000] as const;

/** Waits between retryable manifest notifications and honors caller cancellation. */
export type McpManifestNotifySleep = (delayMs: number, signal?: AbortSignal) => Promise<void>;

/** Represents the structured scalar fields accepted by the connector logger. */
export type McpLogRecord = TetralLogRecord;

type RunMcpToolLogRecordFields = {
  readonly "request.id": string;
  readonly "workspace.id": string;
  readonly "session.id": string;
  readonly "thread.id": string;
  readonly operation: "run_mcp_tool";
  readonly "event.kind": "mcpconnector.call";
  readonly component: "mcp-connector";
  readonly "duration.ms": number;
  readonly mcp_server_name: string;
  readonly tool_name: string;
  readonly status: string;
  readonly error_kind: string;
  readonly refresh_triggered: boolean;
  readonly content_items: number;
  readonly attachment_count: number;
};

type RunMcpToolLogRecord = RunMcpToolLogRecordFields & TetralLogRecord;

/**
 * Collects the admission, execution, durability, notification, observability,
 * and readiness dependencies required by the MCP service shell.
 */
export interface McpConnectorServiceOptions {
  readonly authenticator: McpAuthenticator;
  readonly runtimeBindingTokenVerifier: RuntimeBindingTokenVerifier;
  readonly logger: McpConnectorLogger;
  readonly ready: () => boolean;
  readonly client: McpClient;
  readonly idempotencyStore: McpIdempotencyStore;
  readonly manifestChangeNotifier?: McpManifestChangeNotifier | undefined;
  readonly manifestNotifySleep?: McpManifestNotifySleep | undefined;
  readonly metrics?: McpConnectorMetricsRegistry | undefined;
  readonly activeSessionCount?: (() => number) | undefined;
}

/**
 * Orchestrates MCP discovery and execution for the gRPC server.
 *
 * The shell admits requests before calling MCP, reports each successfully
 * re-listed manifest to Bridge, coordinates Bridge-backed tool result claims
 * and commits, and centralizes terminal metrics and logs. Bridge owns durable
 * manifest identity and readiness decisions. MCP connection and authentication
 * retries remain owned by `McpClient`; durable tool-result replay and
 * post-effect commit remain owned by `McpIdempotencyStore`.
 */
export class McpConnectorServiceShell {
  readonly #idempotencyStore: McpIdempotencyStore;
  readonly #metrics: McpConnectorMetricsRegistry;

  constructor(private readonly options: McpConnectorServiceOptions) {
    this.#idempotencyStore = options.idempotencyStore;
    this.#metrics = options.metrics ?? new McpConnectorMetricsRegistry();
  }

  /**
   * Lists one catalog server's enabled tools for Bridge, removes platform tool
   * collisions, and validates the candidate response. Listing is capture only;
   * it cannot advance the identity acknowledged by durable Bridge state.
   * Family-specific collision filtering remains downstream of this service.
   */
  async listMcpTools(request: ListMcpToolsRequest, metadata: Metadata): Promise<ListMcpToolsResponse> {
    const caller = await this.authorize(metadata, "/tetral.provider_gateway.v1.McpConnectorService/ListMcpTools");
    this.ensureReady();
    const validation = validateListMcpToolsRequest(request);
    if (!validation.ok) {
      throw new GrpcStatusError(status.INVALID_ARGUMENT, validation.message);
    }
    if (catalogEntryByName(request.mcpServerName) === undefined) {
      throw new GrpcStatusError(status.INVALID_ARGUMENT, "invalid internal request");
    }
    const listed = await this.options.client.listTools({
      workspaceId: request.workspaceId,
      sessionId: request.sessionId,
      mcpServerName: request.mcpServerName,
    });
    // The connector does not own the session's pinned builtin family, so those
    // names pass through for Bridge to filter. Platform tool names are
    // family-independent and can be omitted here.
    const { tools, omittedTools } = filterManifestTools(listed, PlatformBuiltinToolNames);
    const response: ListMcpToolsResponse = {
      tools,
      omittedTools,
      manifestEtag: manifestEtag(tools),
    };
    const responseValidation = validateListMcpToolsResponse(response);
    if (!responseValidation.ok) {
      throw new GrpcStatusError(status.INTERNAL, "mcp connector response contract violation");
    }
    this.#metrics.recordManifestRefresh();
    for (const omitted of omittedTools) {
      this.options.logger.info({
        event: "mcp_manifest_tool_omitted",
        severity: "warning",
        operation: "mcp_manifest_list",
        component: "mcp-connector",
        "event.kind": "mcp_manifest_tool_omitted",
        "grpc.method": "/tetral.provider_gateway.v1.McpConnectorService/ListMcpTools",
        "caller.service_account": `${caller.serviceAccount.namespace}/${caller.serviceAccount.name}`,
        "workspace.id": request.workspaceId,
        "session.id": request.sessionId,
        "mcp.server.name": request.mcpServerName,
        "mcp.tool.name": omitted.name,
        "mcp.omission.reason": omitted.reason,
      });
    }
    this.options.logger.info({
      event: "mcp_manifest_listed",
      "event.kind": "mcp_manifest_listed",
      operation: "mcp_manifest_list",
      component: "mcp-connector",
      "grpc.method": "/tetral.provider_gateway.v1.McpConnectorService/ListMcpTools",
      "caller.service_account": `${caller.serviceAccount.namespace}/${caller.serviceAccount.name}`,
      "workspace.id": request.workspaceId,
      "session.id": request.sessionId,
      "mcp.server.name": request.mcpServerName,
      "mcp.manifest_etag": response.manifestEtag,
      "mcp.tool_count": response.tools.length,
    });
    return response;
  }

  /**
   * Re-lists tools after every MCP list-change notification and reports each
   * successfully listed platform-filtered manifest to Bridge. Bridge alone
   * decides whether the durable identity is current or requires progression.
   *
   * Retryable notification failures follow the fixed four-attempt schedule.
   * Exhaustion emits an error record and does not change service readiness.
   */
  async handleToolsListChangedNotification(input: {
    readonly workspaceId: string;
    readonly sessionId: string;
    readonly mcpServerName: string;
  }, signal?: AbortSignal): Promise<{ readonly status: "notified" | "exhausted"; readonly manifestEtag: string; readonly duplicate?: boolean | undefined }> {
    this.ensureReady();
    const validation = validateListMcpToolsRequest(input);
    if (!validation.ok || catalogEntryByName(input.mcpServerName) === undefined) {
      throw new GrpcStatusError(status.INVALID_ARGUMENT, "invalid internal request");
    }
    const listed = await this.options.client.listTools({
      workspaceId: input.workspaceId,
      sessionId: input.sessionId,
      mcpServerName: input.mcpServerName,
    });
    this.#metrics.recordManifestRefresh();
    const { tools } = filterManifestTools(listed, PlatformBuiltinToolNames);
    const nextManifestEtag = manifestEtag(tools);
    if (this.options.manifestChangeNotifier === undefined) {
      throw new GrpcStatusError(status.FAILED_PRECONDITION, "mcp manifest notifier is unavailable");
    }
    let lastFailure: { readonly code: string; readonly message: string } | undefined;
    for (let attempt = 0; attempt <= MCP_MANIFEST_NOTIFY_RETRY_DELAYS_MS.length; attempt += 1) {
      const notified = await this.options.manifestChangeNotifier.notify({
        workspaceId: input.workspaceId,
        sessionId: input.sessionId,
        mcpServerName: input.mcpServerName,
        manifestEtag: nextManifestEtag,
      });
      if (notified.ok) {
        this.options.logger.info({
          event: "mcp_manifest_changed_notified",
          "event.kind": "mcp_manifest_changed_notified",
          operation: "mcp_manifest_changed",
          component: "mcp-connector",
          "workspace.id": input.workspaceId,
          "session.id": input.sessionId,
          "mcp.server.name": input.mcpServerName,
          "mcp.manifest_etag": nextManifestEtag,
          "mcp.notification_duplicate": notified.duplicate,
        });
        return { status: "notified", manifestEtag: nextManifestEtag, duplicate: notified.duplicate };
      }
      if (!notified.retryable) {
        throw new GrpcStatusError(status.FAILED_PRECONDITION, notified.message);
      }
      lastFailure = notified;
      const delayMs = MCP_MANIFEST_NOTIFY_RETRY_DELAYS_MS[attempt];
      if (delayMs !== undefined) {
        await (this.options.manifestNotifySleep ?? abortableManifestNotifySleep)(delayMs, signal);
      }
    }
    this.options.logger.error({
      event: "mcp_manifest_notify_exhausted",
      "event.kind": "mcp_manifest_notify_exhausted",
      operation: "mcp_manifest_changed",
      component: "mcp-connector",
      "workspace.id": input.workspaceId,
      "session.id": input.sessionId,
      "mcp.server.name": input.mcpServerName,
      "mcp.manifest_etag": nextManifestEtag,
      "mcp.notification_attempts": MCP_MANIFEST_NOTIFY_RETRY_DELAYS_MS.length + 1,
      ...semanticErrorFields({
        errorClass: "mcp_manifest_notify_failed",
        errorCode: lastFailure?.code ?? "bridge_unavailable",
        messageSafe: "mcp manifest Bridge notification retries exhausted",
      }),
    });
    return { status: "exhausted", manifestEtag: nextManifestEtag };
  }

  /**
   * Runs one Runtime MCP tool request behind caller authentication, binding
   * verification, catalog admission, and Bridge-backed idempotency.
   *
   * Each returned response or thrown terminal failure records exactly one
   * terminal metric and log. The MCP client owns connection retries, while the
   * idempotency store owns durable claim, replay, and commit.
   */
  async runMcpTool(request: RunMcpToolRequest, metadata: Metadata): Promise<RunMcpToolResponse> {
    const started = performance.now();
    try {
      return await this.runMcpToolCore(request, metadata, started);
    } catch (error) {
      this.recordRunToolFailure(request, runMcpToolThrownErrorKind(error), started);
      throw error;
    }
  }

  private async runMcpToolCore(request: RunMcpToolRequest, metadata: Metadata, started: number): Promise<RunMcpToolResponse> {
    const caller = await this.authorize(metadata, "/tetral.provider_gateway.v1.McpConnectorService/RunMcpTool");
    this.ensureReady();
    const validation = validateRunMcpToolRequest(request);
    if (!validation.ok) {
      throw new GrpcStatusError(status.INVALID_ARGUMENT, validation.message);
    }
    if (!this.options.runtimeBindingTokenVerifier.verify({
      request,
      runtimeBindingToken: request.runtimeBindingToken,
      runtimePodUid: caller.serviceAccount.podUid,
    })) {
      throw new GrpcStatusError(status.PERMISSION_DENIED, "runtime binding token rejected");
    }
    if (catalogEntryByName(request.mcpServerName) === undefined) {
      throw new GrpcStatusError(status.INVALID_ARGUMENT, "invalid internal request");
    }
    const inputHash = normalizedInputHash(request.inputJson);
    const idempotencyContext = mcpIdempotencyContext(request, caller.serviceAccount.podUid);
    const idempotencyKey = {
      toolUseEventId: request.toolUseEventId,
      normalizedInputHash: inputHash,
    };
    const claim = await this.#idempotencyStore.claim(idempotencyKey, idempotencyContext);
    if (claim.status === "replay") {
      return this.finishRunMcpTool(request, claim.stored.response, claim.stored, started);
    }
    if (claim.status === "wait") {
      const stored = await claim.stored;
      if (stored === undefined) {
        return this.finishRunMcpTool(request, mcpIdempotencyRuntimeError("mcp_in_flight", "MCP tool result is still in flight."), { contentItems: 0, refreshTriggered: false }, started);
      }
      return this.finishRunMcpTool(request, stored.response, stored, started);
    }
    if (claim.status === "in_flight") {
      const response: RunMcpToolResponse = {
        status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_RUNTIME_ERROR,
        resultText: "MCP tool result is already in flight.",
        attachments: [],
        errorKind: McpErrorKind.MCP_ERROR_KIND_IN_FLIGHT,
        retryStatus: McpRetryStatus.MCP_RETRY_STATUS_RETRYING,
      };
      const execution = { contentItems: 0, refreshTriggered: false };
      return this.finishRunMcpTool(request, response, execution, started);
    }
    if (claim.status === "stale_custody") {
      return this.finishRunMcpTool(
        request,
        mcpIdempotencyRuntimeError("mcp_custody_lost", "MCP tool execution lost runtime custody."),
        { contentItems: 0, refreshTriggered: false },
        started,
      );
    }
    if (claim.status === "conflict") {
      return this.finishRunMcpTool(request, mcpIdempotencyRuntimeError("mcp_claim_conflict", "MCP tool idempotency conflict."), { contentItems: 0, refreshTriggered: false }, started);
    }
    try {
      const parsedInput = JSON.parse(request.inputJson) as Record<string, unknown>;
      const execution = await this.executeTool(request, parsedInput);
      const response = execution.response;
      const responseValidation = validatePendingRunMcpToolResponse(response);
      if (!responseValidation.ok) {
        throw new GrpcStatusError(status.INTERNAL, "mcp connector response contract violation");
      }
      try {
        const storedResponse = await this.#idempotencyStore.store(idempotencyKey, {
          response,
          contentItems: execution.contentItems,
          refreshTriggered: execution.refreshTriggered,
        }, idempotencyContext);
        return this.finishRunMcpTool(request, storedResponse, execution, started);
      } catch (error) {
        await this.#idempotencyStore.fail(idempotencyKey, idempotencyContext);
        if (error instanceof McpIdempotencyStaleCustodyError) {
          return this.finishRunMcpTool(
            request,
            mcpIdempotencyRuntimeError("mcp_custody_lost", "MCP tool execution lost runtime custody."),
            execution,
            started,
          );
        }
        return this.finishRunMcpTool(request, mcpIdempotencyRuntimeError("mcp_commit_failed", "MCP tool result could not be committed."), execution, started);
      }
    } catch (error) {
      await this.#idempotencyStore.fail(idempotencyKey, idempotencyContext);
      throw error;
    }
  }

  private async executeTool(request: RunMcpToolRequest, input: Record<string, unknown>): Promise<{
    readonly response: PendingRunMcpToolResponse;
    readonly contentItems: number;
    readonly refreshTriggered: boolean;
  }> {
    try {
      const result = await this.options.client.callTool({
        workspaceId: request.workspaceId,
        sessionId: request.sessionId,
        sessionThreadId: request.sessionThreadId,
        mcpServerName: request.mcpServerName,
        toolName: request.toolName,
        input,
      });
      const formatted = formatMcpToolResult(result);
      if (result.isError === true) {
        return {
          response: {
            status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_TOOL_ERROR,
            resultText: formatted.resultText,
            attachments: [...formatted.attachments],
            errorKind: mcpErrorKind("mcp_tool_error"),
            retryStatus: undefined,
          },
          contentItems: result.content?.length ?? 0,
          refreshTriggered: result.refreshTriggered === true,
        };
      }
      return {
        response: {
          status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED,
          resultText: formatted.resultText,
          attachments: [...formatted.attachments],
          errorKind: undefined,
          retryStatus: undefined,
        },
        contentItems: result.content?.length ?? 0,
        refreshTriggered: result.refreshTriggered === true,
      };
    } catch (error) {
      if (error instanceof McpConnectorError) {
        return {
          response: {
            status: modelVisibleMcpError(error.code) ? RunMcpToolStatus.RUN_MCP_TOOL_STATUS_TOOL_ERROR : RunMcpToolStatus.RUN_MCP_TOOL_STATUS_RUNTIME_ERROR,
            resultText: error.message,
            attachments: [],
            errorKind: mcpErrorKind(error.code),
            retryStatus: mcpRetryStatus(error.retryStatus),
          },
          contentItems: 0,
          refreshTriggered: false,
        };
      }
      return {
        response: {
          status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_RUNTIME_ERROR,
          resultText: "MCP connector failed.",
          attachments: [],
          errorKind: mcpErrorKind("mcp_internal_error"),
          retryStatus: undefined,
        },
        contentItems: 0,
        refreshTriggered: false,
      };
    }
  }

  private async authorize(metadata: Metadata, method: string): Promise<{ readonly serviceAccount: { readonly namespace: string; readonly name: string; readonly podUid: string } }> {
    const caller = await this.options.authenticator.authenticate({ metadata, method });
    if (!caller.ok) {
      throw new GrpcStatusError(caller.code === "Unauthenticated" ? status.UNAUTHENTICATED : status.PERMISSION_DENIED, caller.message);
    }
    return caller;
  }

  private ensureReady(): void {
    if (!this.options.ready()) {
      throw new GrpcStatusError(status.FAILED_PRECONDITION, "mcp connector not ready");
    }
  }

  /** Renders connector metrics after refreshing the active-session gauge. */
  metricsText(): string {
    const activeSessionCount = this.options.activeSessionCount ?? this.options.client.connectionCount;
    if (activeSessionCount !== undefined) {
      this.#metrics.setSessionsActive(activeSessionCount());
    }
    return this.#metrics.render();
  }

  private recordRunToolMetrics(
    request: RunMcpToolRequest,
    response: RunMcpToolResponse,
    started: number,
  ): void {
    this.#metrics.recordRunTool({
      tool: request.toolName,
      status: runToolMetricStatus(response.status),
      errorKind: metricErrorKind(response.errorKind),
      durationSeconds: Math.max(0, performance.now() - started) / 1000,
    });
  }

  private finishRunMcpTool(
    request: RunMcpToolRequest,
    response: RunMcpToolResponse,
    execution: { readonly contentItems: number; readonly refreshTriggered: boolean },
    started: number,
  ): RunMcpToolResponse {
    this.recordRunToolMetrics(request, response, started);
    this.options.logger.info(runMcpToolLogRecord(request, response, execution, started));
    return response;
  }

  private recordRunToolFailure(request: RunMcpToolRequest, errorCode: string, started: number): void {
    this.#metrics.recordRunTool({
      tool: request.toolName,
      status: "runtime_error",
      errorKind: errorCode,
      durationSeconds: Math.max(0, performance.now() - started) / 1000,
    });
    this.options.logger.info(runMcpToolFailureLogRecord(request, errorCode, started));
  }
}

function abortableManifestNotifySleep(delayMs: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted === true) {
    return Promise.reject(new GrpcStatusError(status.CANCELLED, "mcp manifest notification cancelled"));
  }
  return new Promise((resolve, reject) => {
    const finish = (): void => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    };
    const timer = setTimeout(finish, delayMs);
    const onAbort = (): void => {
      clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
      reject(new GrpcStatusError(status.CANCELLED, "mcp manifest notification cancelled"));
    };
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

function runMcpToolThrownErrorKind(error: unknown): string {
  if (error instanceof McpConnectorError) {
    return error.code;
  }
  if (error instanceof GrpcStatusError) {
    switch (error.code) {
      case status.UNAUTHENTICATED:
        return "mcp_unauthenticated";
      case status.PERMISSION_DENIED:
        return "runtime_binding_token_rejected";
      case status.FAILED_PRECONDITION:
        return "mcp_not_ready";
      case status.INVALID_ARGUMENT:
        return "mcp_invalid_input";
      default:
        return "mcp_transport_failed";
    }
  }
  return "mcp_connection_failed";
}

function runMcpToolLogRecord(
  request: RunMcpToolRequest,
  response: RunMcpToolResponse,
  execution: { readonly contentItems: number; readonly refreshTriggered: boolean },
  started: number,
): RunMcpToolLogRecord {
  const status = runToolMetricStatus(response.status);
  const errorCode = response.errorKind === undefined ? "" : mcpErrorKindName(response.errorKind);
  const durationMs = Math.round(performance.now() - started);
  const record: RunMcpToolLogRecord = {
    "request.id": request.requestId,
    "workspace.id": request.workspaceId,
    "session.id": request.sessionId,
    "thread.id": request.sessionThreadId,
    operation: "run_mcp_tool",
    "event.kind": "mcpconnector.call",
    component: "mcp-connector",
    "duration.ms": durationMs,
    mcp_server_name: request.mcpServerName,
    tool_name: request.toolName,
    status,
    error_kind: errorCode,
    refresh_triggered: execution.refreshTriggered,
    content_items: execution.contentItems,
    attachment_count: response.attachments.length,
  };
  if (errorCode !== "") {
    return {
      ...record,
      ...semanticErrorFields({ errorClass: errorCode, errorCode, messageSafe: "MCP connector call failed." }),
    };
  }
  return record;
}

function runMcpToolFailureLogRecord(
  request: RunMcpToolRequest,
  errorCode: string,
  started: number,
): RunMcpToolLogRecord {
  return {
    "request.id": request.requestId,
    "workspace.id": request.workspaceId,
    "session.id": request.sessionId,
    "thread.id": request.sessionThreadId,
    operation: "run_mcp_tool",
    "event.kind": "mcpconnector.call",
    component: "mcp-connector",
    "duration.ms": Math.round(performance.now() - started),
    mcp_server_name: request.mcpServerName,
    tool_name: request.toolName,
    status: "runtime_error",
    error_kind: errorCode,
    refresh_triggered: false,
    content_items: 0,
    attachment_count: 0,
    ...semanticErrorFields({ errorClass: errorCode, errorCode, messageSafe: "MCP connector call failed." }),
  };
}

/** Carries an explicit gRPC status from service admission to the server adapter. */
export class GrpcStatusError extends Error {
  constructor(
    readonly code: status,
    message: string,
  ) {
    super(message);
    this.name = "GrpcStatusError";
  }
}

function mcpIdempotencyContext(request: RunMcpToolRequest, runtimePodUid: string): McpIdempotencyContext {
  return {
    requestId: request.requestId,
    workspaceId: request.workspaceId,
    sessionId: request.sessionId,
    sessionThreadId: request.sessionThreadId,
    bindingId: request.bindingId,
    bindingGeneration: request.bindingGeneration,
    runtimePodUid,
    mcpServerName: request.mcpServerName,
    toolName: request.toolName,
    inputJson: request.inputJson,
  };
}

/**
 * Produces the connector-owned manifest subset from discovered tools.
 *
 * Disabled tools are absent, platform-name collisions are reported as
 * omissions, and family-specific names pass through for downstream filtering.
 */
export function filterManifestTools(
  tools: readonly McpClientTool[],
  builtinToolNames: ReadonlySet<string>,
): Pick<ListMcpToolsResponse, "tools" | "omittedTools"> {
  const emitted: McpToolDefinition[] = [];
  const omittedTools: ListMcpToolsResponse["omittedTools"] = [];
  for (const tool of tools) {
    if (tool.enabled === false) {
      continue;
    }
    if (builtinToolNames.has(tool.name)) {
      omittedTools.push({ name: tool.name, reason: "builtin_name_collision" });
      continue;
    }
    emitted.push({
      name: tool.name,
      description: tool.description,
      inputSchemaJson: canonicalJson(tool.inputSchema),
    });
  }
  return { tools: emitted, omittedTools };
}

/** Computes a stable SHA-256 identity over the canonical logical tool manifest. */
export function manifestEtag(tools: readonly McpToolDefinition[]): string {
  return createHash("sha256").update(canonicalJson(tools.map((tool) => ({
    name: tool.name,
    description: tool.description,
    input_schema: JSON.parse(tool.inputSchemaJson) as unknown,
  })))).digest("hex");
}

const PlatformBuiltinToolNames = new Set([
  "web",
  "memory",
  "spawn_agent",
  "send_message",
  "wait_agent",
  "interrupt_agent",
  "close_agent",
  "resume_agent",
  "list_agents",
]);

function mcpRetryStatus(status: McpConnectorError["retryStatus"]): McpRetryStatus | undefined {
  switch (status) {
    case "retrying":
      return McpRetryStatus.MCP_RETRY_STATUS_RETRYING;
    case "exhausted":
      return McpRetryStatus.MCP_RETRY_STATUS_EXHAUSTED;
    case "terminal":
      return McpRetryStatus.MCP_RETRY_STATUS_TERMINAL;
    case undefined:
      return undefined;
  }
}

function modelVisibleMcpError(code: McpConnectorErrorCode): boolean {
  return code === "mcp_tool_error" || code === "mcp_invalid_input" || code === "mcp_timeout";
}

function mcpErrorKindName(kind: McpErrorKind): McpConnectorErrorCode | "" {
  switch (kind) {
    case McpErrorKind.MCP_ERROR_KIND_TOOL_ERROR:
      return "mcp_tool_error";
    case McpErrorKind.MCP_ERROR_KIND_INVALID_INPUT:
      return "mcp_invalid_input";
    case McpErrorKind.MCP_ERROR_KIND_CONNECTION_FAILED:
      return "mcp_connection_failed";
    case McpErrorKind.MCP_ERROR_KIND_AUTHENTICATION_FAILED:
      return "mcp_authentication_failed";
    case McpErrorKind.MCP_ERROR_KIND_CREDENTIAL_REQUIRED:
      return "mcp_credential_required";
    case McpErrorKind.MCP_ERROR_KIND_TIMEOUT:
      return "mcp_timeout";
    case McpErrorKind.MCP_ERROR_KIND_CLAIM_CONFLICT:
      return "mcp_claim_conflict";
    case McpErrorKind.MCP_ERROR_KIND_IN_FLIGHT:
      return "mcp_in_flight";
    case McpErrorKind.MCP_ERROR_KIND_COMMIT_FAILED:
      return "mcp_commit_failed";
    case McpErrorKind.MCP_ERROR_KIND_CUSTODY_LOST:
      return "mcp_custody_lost";
    case McpErrorKind.MCP_ERROR_KIND_INTERNAL:
      return "mcp_internal_error";
    default:
      return "";
  }
}

function mcpIdempotencyRuntimeError(code: "mcp_claim_conflict" | "mcp_in_flight" | "mcp_commit_failed" | "mcp_custody_lost", message: string): RunMcpToolResponse {
  return {
    status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_RUNTIME_ERROR,
    resultText: message,
    attachments: [],
    errorKind: mcpErrorKind(code),
    retryStatus: code === "mcp_custody_lost"
      ? undefined
      : code === "mcp_claim_conflict"
        ? McpRetryStatus.MCP_RETRY_STATUS_TERMINAL
        : McpRetryStatus.MCP_RETRY_STATUS_RETRYING,
  };
}

function runToolMetricStatus(status: RunMcpToolStatus): McpConnectorRunToolMetric["status"] {
  switch (status) {
    case RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED:
      return "completed";
    case RunMcpToolStatus.RUN_MCP_TOOL_STATUS_TOOL_ERROR:
      return "tool_error";
    case RunMcpToolStatus.RUN_MCP_TOOL_STATUS_RUNTIME_ERROR:
      return "runtime_error";
    default:
      return "unspecified";
  }
}

function metricErrorKind(errorKind: McpErrorKind | undefined): string {
  if (errorKind === undefined) {
    return "";
  }
  if (errorKind === McpErrorKind.MCP_ERROR_KIND_INTERNAL) {
    return "mcp_internal_error";
  }
  return mcpErrorKindName(errorKind);
}
