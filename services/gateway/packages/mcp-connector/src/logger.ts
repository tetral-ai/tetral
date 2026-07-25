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
