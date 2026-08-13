/**
 * @packageDocumentation
 *
 * Defines canonical MCP request identity and the tool-result states shared by the connector
 * service and its Bridge adapter. Callers hash parsed object JSON before claiming an external
 * side effect, serialize pending results for Bridge, and parse only refs-only committed results
 * for replay. The module guards payload conflicts for one tool-use event, separates inline media
 * from durable attachment references, and clones replay values so consumers cannot mutate cached
 * state.
 */
import { createHash } from "node:crypto";
import type { RunMcpToolResponse } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { McpInlineMedia } from "./formatter.js";

/** Pairs a public tool-use event with the canonical hash of its MCP argument object. */
export interface IdempotencyKey {
  readonly toolUseEventId: string;
  readonly normalizedInputHash: string;
}

/** Carries the tenant, binding, caller-pod, server, tool, and request fields Bridge binds to a claim. */
export interface McpIdempotencyContext {
  readonly requestId: string;
  readonly workspaceId: string;
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly bindingId: string;
  readonly bindingGeneration: number;
  readonly runtimePodUid: string;
  readonly mcpServerName: string;
  readonly toolName: string;
  readonly inputJson: string;
}

/** Records connector observations that accompany a stored tool response and its replay. */
export interface RunMcpToolObservation {
  readonly contentItems: number;
  readonly refreshTriggered: boolean;
}

/** Combines a refs-only committed response with the observations preserved for replay logging. */
export interface StoredRunMcpToolResponse extends RunMcpToolObservation {
  readonly response: RunMcpToolResponse;
}

/** Represents a formatted tool response whose attachments still contain bounded in-memory bytes. */
export interface PendingRunMcpToolResponse {
  readonly status: RunMcpToolResponse["status"];
  readonly resultText: string;
  readonly attachments: readonly McpInlineMedia[];
  readonly errorKind?: RunMcpToolResponse["errorKind"];
  readonly retryStatus?: RunMcpToolResponse["retryStatus"];
}

/** Combines a pre-commit response with the observations Bridge persists beside its refs-only form. */
export interface PendingStoredRunMcpToolResponse extends RunMcpToolObservation {
  readonly response: PendingRunMcpToolResponse;
}

/**
 * Describes whether a canonical tool request may execute, replays a result, waits on local work,
 * observes a remote reservation, or conflicts with a different payload.
 */
export type IdempotencyClaim =
  | { readonly status: "new" }
  | { readonly status: "replay"; readonly stored: StoredRunMcpToolResponse }
  | { readonly status: "wait"; readonly stored: Promise<StoredRunMcpToolResponse | undefined> }
  | { readonly status: "in_flight" }
  | { readonly status: "stale_custody" }
  | { readonly status: "conflict" };

/** Signals that Connector cannot prove current MCP claim custody and must defer to reconciliation. */
export class McpIdempotencyStaleCustodyError extends Error {
  constructor() {
    super("mcp tool result belongs to stale runtime custody");
    this.name = "McpIdempotencyStaleCustodyError";
  }
}

/**
 * Defines the claim, commit, and failure lifecycle surrounding one external MCP side effect.
 *
 * Implementations reserve before execution, materialize a refs-only response on commit, and
 * release unresolved local work after failure. Durable implementations require the full context.
 */
export interface McpIdempotencyStore {
  claim(key: IdempotencyKey, context?: McpIdempotencyContext): IdempotencyClaim | Promise<IdempotencyClaim>;
  store(key: IdempotencyKey, stored: PendingStoredRunMcpToolResponse, context?: McpIdempotencyContext): RunMcpToolResponse | Promise<RunMcpToolResponse>;
  fail(key: IdempotencyKey, context?: McpIdempotencyContext): void | Promise<void>;
}

type InFlightResponse = {
  readonly status: "in_flight";
  readonly inputHash: string;
  readonly promise: Promise<StoredRunMcpToolResponse | undefined>;
  readonly resolve: (stored: StoredRunMcpToolResponse | undefined) => void;
};

type CompletedResponse = {
  readonly status: "completed";
  readonly inputHash: string;
  readonly stored: StoredRunMcpToolResponse;
};

/**
 * Coalesces pending calls and replays already materialized MCP results within one process.
 *
 * Its store path rejects pending media because only Bridge can create durable attachment
 * references; it detects different canonical hashes for the same tool-use event and returns
 * cloned responses.
 */
export class InMemoryMcpIdempotencyStore implements McpIdempotencyStore {
  readonly #responses = new Map<string, InFlightResponse | CompletedResponse>();

  claim(key: IdempotencyKey, context?: McpIdempotencyContext): IdempotencyClaim {
    const mapKey = idempotencyMapKey(key, context);
    const existing = this.#responses.get(mapKey);
    if (existing === undefined) {
      let resolve!: (stored: StoredRunMcpToolResponse | undefined) => void;
      const promise = new Promise<StoredRunMcpToolResponse | undefined>((settle) => {
        resolve = settle;
      });
      this.#responses.set(mapKey, {
        status: "in_flight",
        inputHash: key.normalizedInputHash,
        promise,
        resolve,
      });
      return { status: "new" };
    }
    if (existing.inputHash !== key.normalizedInputHash) {
      return { status: "conflict" };
    }
    if (existing.status === "in_flight") {
      return {
        status: "wait",
        stored: existing.promise.then((stored) => stored === undefined ? undefined : cloneStoredResponse(stored)),
      };
    }
    return { status: "replay", stored: cloneStoredResponse(existing.stored) };
  }

  store(key: IdempotencyKey, stored: PendingStoredRunMcpToolResponse, context?: McpIdempotencyContext): RunMcpToolResponse {
    if (stored.response.attachments.length > 0) {
      throw new Error("in-memory MCP idempotency cannot materialize media refs");
    }
    return this.storeCompleted(key, {
      response: {
        status: stored.response.status,
        resultText: stored.response.resultText,
        attachments: [],
        errorKind: stored.response.errorKind,
        retryStatus: stored.response.retryStatus,
      },
      contentItems: stored.contentItems,
      refreshTriggered: stored.refreshTriggered,
    }, context);
  }

  storeCompleted(key: IdempotencyKey, stored: StoredRunMcpToolResponse, context?: McpIdempotencyContext): RunMcpToolResponse {
    const mapKey = idempotencyMapKey(key, context);
    const frozen = cloneStoredResponse(stored);
    const existing = this.#responses.get(mapKey);
    this.#responses.set(mapKey, {
      status: "completed",
      inputHash: key.normalizedInputHash,
      stored: frozen,
    });
    if (existing?.status === "in_flight" && existing.inputHash === key.normalizedInputHash) {
      existing.resolve(cloneStoredResponse(frozen));
    }
    return cloneResponse(frozen.response);
  }

  fail(key: IdempotencyKey, context?: McpIdempotencyContext): void {
    const mapKey = idempotencyMapKey(key, context);
    const existing = this.#responses.get(mapKey);
    if (existing?.status === "in_flight" && existing.inputHash === key.normalizedInputHash) {
      this.#responses.delete(mapKey);
      existing.resolve(undefined);
    }
  }
}

function idempotencyMapKey(key: IdempotencyKey, context?: McpIdempotencyContext): string {
  if (context === undefined) {
    return key.toolUseEventId;
  }
  return `${context.workspaceId}\u0000${context.sessionId}\u0000${context.sessionThreadId}\u0000${key.toolUseEventId}`;
}

/** Parses an MCP argument object, canonicalizes it, and returns its lowercase SHA-256 identity. */
export function normalizedInputHash(inputJson: string): string {
  return createHash("sha256").update(canonicalJson(parseJSONObject(inputJson))).digest("hex");
}

/**
 * Parses JSON and requires an object at the top level.
 *
 * @throws When the input is invalid JSON, null, an array, or a scalar.
 */
export function parseJSONObject(inputJson: string): Record<string, unknown> {
  const parsed = JSON.parse(inputJson) as unknown;
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error("input_json must be a JSON object");
  }
  return parsed as Record<string, unknown>;
}

/**
 * Serializes JSON-compatible data with recursively sorted object keys and preserved array order.
 *
 * Callers use this encoding for request hashes, input schemas, structured content, and manifest
 * identities; values originate from parsed JSON or protocol-compatible object graphs.
 */
export function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map((entry) => canonicalJson(entry)).join(",")}]`;
  }
  if (value !== null && typeof value === "object") {
    const object = value as Record<string, unknown>;
    return `{${Object.keys(object).sort().map((key) => `${JSON.stringify(key)}:${canonicalJson(object[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

/** Serializes a pre-commit response as metadata plus attachment descriptors, never inline bytes. */
export function serializePendingRunMcpToolResponse(stored: PendingStoredRunMcpToolResponse): string {
  return JSON.stringify({
    response: {
      status: stored.response.status,
      result_text: stored.response.resultText,
      attachments: stored.response.attachments.map((attachment) => ({
        mime: attachment.mime,
        size_bytes: attachment.data.byteLength,
        suggested_filename: attachment.suggestedFilename,
      })),
      error_kind: stored.response.errorKind ?? null,
      retry_status: stored.response.retryStatus ?? null,
    },
    content_items: stored.contentItems,
    refresh_triggered: stored.refreshTriggered,
  });
}

/**
 * Parses a Bridge-stored refs-only response and its replay observations.
 *
 * @throws When required response, attachment-reference, or observation fields have invalid shapes.
 */
export function parseStoredRunMcpToolResponse(value: string): StoredRunMcpToolResponse {
  const parsed = JSON.parse(value) as unknown;
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("invalid stored MCP tool response");
  }
  const record = parsed as Record<string, unknown>;
  const response = record.response;
  if (response === null || typeof response !== "object" || Array.isArray(response)) {
    throw new Error("invalid stored MCP tool response");
  }
  const responseRecord = response as Record<string, unknown>;
  const attachments = responseRecord.attachments;
  if (
    typeof responseRecord.status !== "number" ||
    typeof responseRecord.result_text !== "string" ||
    !Array.isArray(attachments) ||
    typeof record.content_items !== "number" ||
    typeof record.refresh_triggered !== "boolean"
  ) {
    throw new Error("invalid stored MCP tool response");
  }
  return {
    response: {
      status: responseRecord.status,
      resultText: responseRecord.result_text,
      attachments: attachments.map((attachment) => parseStoredAttachment(attachment)),
      errorKind: typeof responseRecord.error_kind === "number" ? responseRecord.error_kind : undefined,
      retryStatus: typeof responseRecord.retry_status === "number" ? responseRecord.retry_status : undefined,
      materializationHandle: "",
    },
    contentItems: record.content_items,
    refreshTriggered: record.refresh_triggered,
  };
}

function parseStoredAttachment(value: unknown): RunMcpToolResponse["attachments"][number] {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("invalid stored MCP tool response");
  }
  const record = value as Record<string, unknown>;
  if (
    typeof record.attachment_ref !== "string" || record.attachment_ref.length === 0 ||
    typeof record.mime !== "string" ||
    typeof record.size_bytes !== "number" || !Number.isInteger(record.size_bytes) || record.size_bytes <= 0
  ) {
    throw new Error("invalid stored MCP tool response");
  }
  return {
    attachmentRef: record.attachment_ref,
    mime: record.mime,
    sizeBytes: record.size_bytes,
    suggestedFilename: typeof record.suggested_filename === "string" ? record.suggested_filename : "",
  };
}

/** Returns a replay-safe copy so callers cannot mutate the stored response or its attachments. */
export function cloneResponse(response: RunMcpToolResponse): RunMcpToolResponse {
  return {
    status: response.status,
    resultText: response.resultText,
    attachments: response.attachments.map((attachment) => ({
      attachmentRef: attachment.attachmentRef,
      mime: attachment.mime,
      sizeBytes: attachment.sizeBytes,
      suggestedFilename: attachment.suggestedFilename,
    })),
    errorKind: response.errorKind,
    retryStatus: response.retryStatus,
    materializationHandle: response.materializationHandle,
  };
}

/** Copies both the refs-only response and its connector observations for replay isolation. */
export function cloneStoredResponse(stored: StoredRunMcpToolResponse): StoredRunMcpToolResponse {
  return {
    response: cloneResponse(stored.response),
    contentItems: stored.contentItems,
    refreshTriggered: stored.refreshTriggered,
  };
}
