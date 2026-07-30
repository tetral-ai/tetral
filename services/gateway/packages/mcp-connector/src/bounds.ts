/**
 * @packageDocumentation
 *
 * Validates MCP connector requests and produced responses at the internal wire boundary. The RPC
 * service calls these validators before opening an MCP call, accepting a discovered manifest, or
 * committing a pending tool result. They apply shared identifier, token, JSON, schema, and text
 * ceilings together with MCP-specific enum and single-attachment rules, and they reject invalid
 * shapes without truncating or normalizing caller data.
 */
import { McpErrorKind, McpRetryStatus, RunMcpToolStatus } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import {
  MaxBindingGeneration,
  MaxIdBytes,
  MaxJsonBytes,
  MaxSchemaBytes,
  MaxTextBytes,
  MaxTokenBytes,
} from "@tetral/gateway-protocol/src/bounds.js";
import type { ListMcpToolsRequest, ListMcpToolsResponse, RunMcpToolRequest, RunMcpToolResponse } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { ValidationResult } from "@tetral/gateway-protocol/src/bounds.js";
import { MCP_BLOB_MAX_BYTES } from "./formatter.js";
import type { PendingRunMcpToolResponse } from "./idempotency.js";

const MaxManifestEtagBytes = 64;

/** Validates identity, binding, server, tool, token, and object-JSON fields for one MCP tool call. */
export function validateRunMcpToolRequest(request: RunMcpToolRequest): ValidationResult {
  if (
    invalidBytes(request.requestId, MaxIdBytes) ||
    invalidBytes(request.workspaceId, MaxIdBytes) ||
    invalidBytes(request.sessionId, MaxIdBytes) ||
    invalidBytes(request.sessionThreadId, MaxIdBytes) ||
    invalidBytes(request.toolUseEventId, MaxIdBytes) ||
    invalidBytes(request.mcpServerName, MaxIdBytes) ||
    invalidBytes(request.toolName, MaxIdBytes) ||
    invalidBytes(request.bindingId, MaxIdBytes) ||
    invalidBindingGeneration(request.bindingGeneration) ||
    invalidBytes(request.runtimeBindingToken, MaxTokenBytes) ||
    !validJsonObject(request.inputJson, MaxJsonBytes)
  ) {
    return invalidRequest();
  }
  return { ok: true };
}

/** Validates the tenant scope and catalog name carried by a tool-discovery request. */
export function validateListMcpToolsRequest(request: ListMcpToolsRequest): ValidationResult {
  if (
    invalidBytes(request.workspaceId, MaxIdBytes) ||
    invalidBytes(request.sessionId, MaxIdBytes) ||
    invalidBytes(request.mcpServerName, MaxIdBytes)
  ) {
    return invalidRequest();
  }
  return { ok: true };
}

/**
 * Validates a formatted pre-commit tool result while media still contains inline bytes.
 *
 * The response must keep status, error, and retry fields inside their closed enum domains;
 * completed responses carry neither error nor retry fields, and any retry status is limited to
 * connection-failed, authentication-failed, or credential-required errors. The result carries no
 * more than one non-empty attachment and keeps aggregate media bytes within the blob ceiling.
 */
export function validatePendingRunMcpToolResponse(response: PendingRunMcpToolResponse): ValidationResult {
  const common = validateRunMcpToolResponseCommon(response);
  if (!common.ok) {
    return common;
  }
  if (response.attachments.length > 1) {
    return invalidRequest();
  }
  let attachmentBytes = 0;
  for (const attachment of response.attachments) {
    attachmentBytes += attachment.data.byteLength;
    if (
      attachment.data.byteLength === 0 ||
      attachment.data.byteLength > MCP_BLOB_MAX_BYTES ||
      attachmentBytes > MCP_BLOB_MAX_BYTES ||
      invalidBytes(attachment.mime, MaxIdBytes) ||
      invalidBytes(attachment.suggestedFilename, MaxIdBytes)
    ) {
      return invalidRequest();
    }
  }
  return { ok: true };
}

function validateRunMcpToolResponseCommon(response: Pick<RunMcpToolResponse, "status" | "resultText" | "errorKind" | "retryStatus">): ValidationResult {
  if (
    !validEnumValue(
      response.status,
      RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED,
      RunMcpToolStatus.RUN_MCP_TOOL_STATUS_TOOL_ERROR,
      RunMcpToolStatus.RUN_MCP_TOOL_STATUS_RUNTIME_ERROR,
    ) ||
    invalidBytesAllowEmpty(response.resultText, MaxTextBytes)
  ) {
    return invalidRequest();
  }
  if (response.status === RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED && response.errorKind !== undefined) {
    return invalidRequest();
  }
  if (response.status === RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED && response.retryStatus !== undefined) {
    return invalidRequest();
  }
  if (
    response.status !== RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED &&
    !validEnumValue(
      response.errorKind ?? McpErrorKind.MCP_ERROR_KIND_UNSPECIFIED,
      McpErrorKind.MCP_ERROR_KIND_TOOL_ERROR,
      McpErrorKind.MCP_ERROR_KIND_INVALID_INPUT,
      McpErrorKind.MCP_ERROR_KIND_CONNECTION_FAILED,
      McpErrorKind.MCP_ERROR_KIND_AUTHENTICATION_FAILED,
      McpErrorKind.MCP_ERROR_KIND_TIMEOUT,
      McpErrorKind.MCP_ERROR_KIND_CREDENTIAL_REQUIRED,
      McpErrorKind.MCP_ERROR_KIND_INTERNAL,
      McpErrorKind.MCP_ERROR_KIND_CUSTODY_LOST,
    )
  ) {
    return invalidRequest();
  }
  if (response.retryStatus !== undefined && !validEnum(response.retryStatus, McpRetryStatus.MCP_RETRY_STATUS_RETRYING, McpRetryStatus.MCP_RETRY_STATUS_TERMINAL)) {
    return invalidRequest();
  }
  if (response.retryStatus !== undefined && !validRetryStatusForErrorKind(response.errorKind)) {
    return invalidRequest();
  }
  return { ok: true };
}

/**
 * Validates a discovered tool manifest before the connector returns it to Bridge.
 *
 * Tool names, descriptions, schemas, the content hash, and explicit collision omissions remain
 * within their field bounds and use only the connector's supported omission reason.
 */
export function validateListMcpToolsResponse(response: ListMcpToolsResponse): ValidationResult {
  if (invalidBytes(response.manifestEtag, MaxManifestEtagBytes)) {
    return invalidRequest();
  }
  for (const tool of response.tools) {
    if (invalidBytes(tool.name, MaxIdBytes) || invalidBytes(tool.description, MaxTextBytes) || !validJsonObject(tool.inputSchemaJson, MaxSchemaBytes)) {
      return invalidRequest();
    }
  }
  for (const omitted of response.omittedTools) {
    if (invalidBytes(omitted.name, MaxIdBytes) || omitted.reason !== "builtin_name_collision") {
      return invalidRequest();
    }
  }
  return { ok: true };
}

function invalidBytes(value: string, limit: number): boolean {
  return value.length === 0 || new TextEncoder().encode(value).byteLength > limit;
}

function invalidBindingGeneration(value: number): boolean {
  return !Number.isInteger(value) || value <= 0 || value > MaxBindingGeneration;
}

function invalidBytesAllowEmpty(value: string, limit: number): boolean {
  return new TextEncoder().encode(value).byteLength > limit;
}

function validEnum(value: number | undefined, min: number, max: number): boolean {
  return value !== undefined && Number.isInteger(value) && value >= min && value <= max;
}

function validRetryStatusForErrorKind(errorKind: McpErrorKind | undefined): boolean {
  return errorKind === McpErrorKind.MCP_ERROR_KIND_CONNECTION_FAILED ||
    errorKind === McpErrorKind.MCP_ERROR_KIND_AUTHENTICATION_FAILED ||
    errorKind === McpErrorKind.MCP_ERROR_KIND_CREDENTIAL_REQUIRED;
}

function validEnumValue(value: number, ...allowed: readonly number[]): boolean {
  return Number.isInteger(value) && allowed.includes(value);
}

function validJsonObject(value: string, limit: number): boolean {
  if (invalidBytes(value, limit)) {
    return false;
  }
  try {
    const parsed = JSON.parse(value) as unknown;
    return typeof parsed === "object" && parsed !== null && !Array.isArray(parsed);
  } catch {
    return false;
  }
}

function invalidRequest(): ValidationResult {
  return { ok: false, message: "invalid internal request" };
}
