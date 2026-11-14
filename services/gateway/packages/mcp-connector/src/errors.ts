/**
 * @packageDocumentation
 *
 * Defines the connector domain errors that serialize into `RunMcpToolResponse`
 * and their one-to-one mapping onto the generated protocol enum. MCP client and
 * orchestration paths create {@link McpConnectorError} values, while the
 * connector service calls {@link mcpErrorKind} for those tool and runtime
 * failures. Terminal logs and metrics reuse these codes; admission and transport
 * paths add further telemetry-only labels outside this response type.
 */

import { McpErrorKind } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { causeOf } from "@tetral/ts-observability";
import type { FailureCause } from "@tetral/ts-observability";

/** Enumerates the connector domain-error codes accepted by {@link mcpErrorKind}. */
export type McpConnectorErrorCode =
  | "mcp_tool_error"
  | "mcp_invalid_input"
  | "mcp_connection_failed"
  | "mcp_authentication_failed"
  | "mcp_credential_required"
  | "mcp_timeout"
  | "mcp_claim_conflict"
  | "mcp_in_flight"
  | "mcp_commit_failed"
  | "mcp_custody_lost"
  | "mcp_internal_error";

/** Maps a connector error code to the corresponding Gateway protocol value. */
export function mcpErrorKind(code: McpConnectorErrorCode): McpErrorKind {
  switch (code) {
    case "mcp_tool_error":
      return McpErrorKind.MCP_ERROR_KIND_TOOL_ERROR;
    case "mcp_invalid_input":
      return McpErrorKind.MCP_ERROR_KIND_INVALID_INPUT;
    case "mcp_connection_failed":
      return McpErrorKind.MCP_ERROR_KIND_CONNECTION_FAILED;
    case "mcp_authentication_failed":
      return McpErrorKind.MCP_ERROR_KIND_AUTHENTICATION_FAILED;
    case "mcp_credential_required":
      return McpErrorKind.MCP_ERROR_KIND_CREDENTIAL_REQUIRED;
    case "mcp_timeout":
      return McpErrorKind.MCP_ERROR_KIND_TIMEOUT;
    case "mcp_claim_conflict":
      return McpErrorKind.MCP_ERROR_KIND_CLAIM_CONFLICT;
    case "mcp_in_flight":
      return McpErrorKind.MCP_ERROR_KIND_IN_FLIGHT;
    case "mcp_commit_failed":
      return McpErrorKind.MCP_ERROR_KIND_COMMIT_FAILED;
    case "mcp_custody_lost":
      return McpErrorKind.MCP_ERROR_KIND_CUSTODY_LOST;
    case "mcp_internal_error":
      return McpErrorKind.MCP_ERROR_KIND_INTERNAL;
  }
}

/**
 * Carries a classified MCP failure and its optional retry settlement state
 * across client and service boundaries.
 */
export class McpConnectorError extends Error {
  constructor(
    readonly code: McpConnectorErrorCode,
    message: string,
    readonly retryStatus?: "retrying" | "exhausted" | "terminal" | undefined,
  ) {
    super(message);
    this.name = "McpConnectorError";
  }
}

/**
 * The cause a record takes for a failed MCP operation: the text of an error
 * this connector made, which `own` tells; of any other error only its kind,
 * since its text may be the MCP server's.
 */
export function mcpFailureCause(error: unknown, own: (error: unknown) => boolean = () => false): FailureCause {
  if (error instanceof McpConnectorError || own(error) || (typeof error === "object" && error !== null && engineDependencyErrors.has(error))) return causeOf(error);
  return causeOf(`an unclassified ${error instanceof Error ? error.name : typeof error} ended the MCP operation`);
}

/** Errors of an Engine dependency (the database, the tool-result store), which hold no MCP server text: a record takes their text as its cause. */
const engineDependencyErrors = new WeakSet<object>();

/** Runs a call to an Engine dependency and marks the error it fails with as one whose text a record may take. */
export async function fromEngineDependency<T>(call: () => T | PromiseLike<T>): Promise<T> {
  try { return await call(); }
  catch (error) {
    if (typeof error === "object" && error !== null) engineDependencyErrors.add(error);
    throw error;
  }
}
