/**
 * @packageDocumentation
 *
 * Adapts the shared JSON observability logger to the MCP connector's fixed
 * service identity and configuration-failure record shape. The process command
 * creates the logger here and uses the helper for rejected configuration;
 * connector components emit records through the returned interface. Other
 * command and cleanup failures use fixed phase classifications at the process
 * entry and signal boundaries. Encoding and output delegate to the shared
 * TypeScript observability package.
 */

import { createTetralJsonLogger, semanticErrorFields } from "@tetral/ts-observability";
import type { DiagnosticConfig, TetralDiagnosticLogger, TetralJsonLogger, TetralLogRecord } from "@tetral/ts-observability";
import type { McpOAuthRefreshCompletedEvent } from "./credential-update-path.js";

/** Defines the structured record shape accepted by the connector logger. */
export type McpConnectorLogRecord = TetralLogRecord;

/**
 * Defines the shared JSON logger specialized for connector records. The
 * process logger also supplies `warn`, which connector warnings use.
 */
export type McpConnectorLogger = TetralJsonLogger<McpConnectorLogRecord> & Partial<Pick<TetralDiagnosticLogger<McpConnectorLogRecord>, "warn">>;

/** Creates a structured logger whose service identity is always `mcp-connector`. */
export function createJsonLogger(options: {
  readonly write: (line: string) => unknown;
  readonly sinkFailures?: (() => number) | undefined;
  readonly diagnostics?: DiagnosticConfig;
  readonly deploymentEnvironment?: string | undefined;
  readonly serviceVersion?: string | undefined;
}): TetralDiagnosticLogger<McpConnectorLogRecord> {
  return createTetralJsonLogger<McpConnectorLogRecord>({
    write: options.write,
    sinkFailures: options.sinkFailures,
    diagnostics: options.diagnostics,
    serviceName: "mcp-connector",
    deploymentEnvironment: options.deploymentEnvironment,
    serviceVersion: options.serviceVersion,
  });
}

/** Builds the safe structured record emitted when startup configuration fails. */
export function startupFailureLogRecord(input: { readonly kind: "config_error"; readonly message: string }): McpConnectorLogRecord {
  return {
    event: "startup_failed",
    "event.kind": "startup_failed",
    operation: "startup",
    component: "mcp-connector",
    kind: input.kind,
    message: input.message,
    ...semanticErrorFields({ errorClass: input.kind, errorCode: input.kind, messageSafe: input.message }),
  };
}

/** Builds the lifecycle record emitted after both connector listeners are ready. */
export function workloadStartedLogRecord(): McpConnectorLogRecord {
  return {
    event: "workload.started",
    "event.kind": "started",
    operation: "workload.lifecycle",
    component: "workload",
    "listener.transport": "tcp",
    "readiness.state": "ready",
  };
}

/** Builds the bounded owner-exit record for one OAuth refresh decision. */
export function mcpOAuthRefreshCompletedLogRecord(event: McpOAuthRefreshCompletedEvent): McpConnectorLogRecord {
  return {
    event: "mcp_oauth_refresh_completed", "event.kind": "mcp_oauth_refresh_completed",
    operation: "mcp_oauth_refresh", component: "mcp-connector",
    message: "MCP OAuth refresh owner completed",
    "workspace.id": event.workspaceId, "session.id": event.sessionId,
    "mcp.server.name": event.mcpServerName, "credential.id": event.credentialId,
    outcome: event.outcome,
    ...(event.failureKind !== undefined ? { "failure.kind": event.failureKind } : {}),
    ...(event.httpStatusClass !== undefined ? { "http.status_class": event.httpStatusClass } : {}),
    "durable_write.disposition": event.durableWrite, "duration.ms": event.durationMs,
  };
}

/** Emits the event and its issuer-attempt metric without joining refresh custody. */
export function recordMcpOAuthRefreshCompleted(
  logger: Pick<McpConnectorLogger, "info"> | undefined,
  recordRefreshAttempt: ((outcome: "success" | "failed") => void) | undefined,
  event: McpOAuthRefreshCompletedEvent,
): void {
  try {
    if (event.refreshAttemptMetric !== undefined) recordRefreshAttempt?.(event.refreshAttemptMetric);
    logger?.info(mcpOAuthRefreshCompletedLogRecord(event));
  } catch {
    // Credential rotation and fail-closed resolution are authoritative.
  }
}

/** Emits the started record without allowing a logging sink to change process readiness. */
export function logWorkloadStarted(logger: McpConnectorLogger): void {
  try {
    logger.info(workloadStartedLogRecord());
  } catch {
    // Listener readiness, not observability delivery, determines startup success.
  }
}

export interface McpExecutionObservation {
  readonly claimId: string; readonly toolUseEventId: string;
  readonly elapsedMs: number; readonly remainingMs: number;
}

/**
 * Fixed owner-phase observation; never derives diagnostics from dependency messages.
 *
 * A phase completion without a claim identity, such as a client phase of explicit
 * discovery or notification re-listing that no claimed execution observes, carries
 * the control-only `diagnostic.repeat` marker for every outcome: `completed`,
 * `failed`, `mcp_timeout` and `mcp_authentication_failed`. The marker admits these
 * Info records to the shared repeated-event limiter and is never written. Limiter
 * windows are keyed by event and reason (here the outcome), so each distinct phase
 * outcome emits its first record and later repeats in the window are summarized.
 */
export function mcpPhaseCompletedLogRecord(input: {
  readonly workspaceId: string; readonly sessionId: string; readonly mcpServerName?: string;
  readonly phase: string; readonly outcome: string; readonly durationMs: number;
  readonly claimId?: string; readonly toolUseEventId?: string;
  readonly elapsedMs?: number; readonly remainingMs?: number; readonly attempt?: number;
}): McpConnectorLogRecord {
  return {event: `mcp_${input.phase}_completed`, "event.kind": `mcp_${input.phase}_completed`,
    operation: `mcp_${input.phase}`, component: "mcp-connector", phase: input.phase,
    outcome: input.outcome, "duration.ms": Math.max(0,input.durationMs),
    "workspace.id": input.workspaceId, "session.id": input.sessionId,
    ...(input.mcpServerName === undefined ? {} : {"mcp.server.name": input.mcpServerName}),
    ...(input.claimId === undefined ? {} : {"request.id": input.claimId}),
    ...(input.toolUseEventId === undefined ? {} : {"mcp.tool_use_event_id": input.toolUseEventId}),
    ...(input.elapsedMs === undefined ? {} : {"timeout.elapsed_ms": Math.max(0,input.elapsedMs)}),
    ...(input.remainingMs === undefined ? {} : {"timeout.remaining_ms": Math.max(0,input.remainingMs)}),
    ...(input.attempt === undefined ? {} : {attempt: input.attempt}),
    "diagnostic.repeat": input.claimId === undefined};
}
