/**
 * @packageDocumentation
 *
 * Adapts the shared JSON observability logger to the MCP connector's fixed
 * service identity and configuration-failure record shape. The process command
 * creates the logger here and uses the helper for rejected configuration;
 * connector components emit records through the returned interface. Other
 * uncaught startup failures remain owned by the process entry point and may
 * bypass this structured helper. Encoding and output delegate to the shared
 * TypeScript observability package.
 */

import { createTetralJsonLogger, semanticErrorFields } from "@tetral/ts-observability";
import type { TetralJsonLogger, TetralLogRecord } from "@tetral/ts-observability";
import type { McpOAuthRefreshCompletedEvent } from "./credential-update-path.js";

/** Defines the structured record shape accepted by the connector logger. */
export type McpConnectorLogRecord = TetralLogRecord;

/** Defines the shared JSON logger specialized for connector records. */
export type McpConnectorLogger = TetralJsonLogger<McpConnectorLogRecord>;

/** Creates a structured logger whose service identity is always `mcp-connector`. */
export function createJsonLogger(options: {
  readonly write: (line: string) => void;
  readonly deploymentEnvironment?: string | undefined;
  readonly serviceVersion?: string | undefined;
}): McpConnectorLogger {
  return createTetralJsonLogger<McpConnectorLogRecord>({
    write: options.write,
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
