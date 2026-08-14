/**
 * @packageDocumentation
 * Projects Runtime-owned conversation state into the provider-only context
 * carrier consumed by Gateway.
 *
 * The projection is the ownership seam: durable Message and Part identity,
 * Runtime status/origin, Tool lifecycle identity, and repair conventions stop
 * here. Gateway receives only ordered provider roles and content. Tool calls and
 * results are independent items paired solely by `modelToolCallId`.
 *
 * Until the durable context model owns this source directly, the projector
 * reads the current Runtime conversation representation. Streaming messages are
 * omitted; public Tool state still requires Runtime-owned Tool identity, while
 * an internal invalid-tool repair is recognized only on this side of the
 * boundary. Calls remain at their first provider position and later terminal
 * snapshots contribute only the paired result. Neither Runtime distinction is
 * encoded in provider context.
 */
import { ProviderContextRole } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type {
  ProviderContextEntry,
  ProviderContextItem,
  ProviderToolResult,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { RuntimeMessage, RuntimePart } from "../contracts/runtime.js";
import type { ProviderError } from "../contracts/provider.js";
import {
  DurableRuntimeMessageSchema,
  RuntimeMessageSchema,
} from "../contracts/runtime.js";
import { normalizeProviderError } from "../contracts/provider.js";

/** Success or normalized provider-input failure from Runtime context projection. */
export type RuntimeProviderContextResult =
  | { readonly ok: true; readonly context: readonly ProviderContextEntry[] }
  | { readonly ok: false; readonly error: ProviderError };

/** Lowers completed Runtime conversation history into provider-only context. */
export function toGatewayProviderContext(input: unknown): RuntimeProviderContextResult {
  if (!Array.isArray(input) || input.length === 0) {
    return { ok: false, error: runtimeInputError("schema") };
  }
  const parsedMessages: RuntimeMessage[] = [];
  for (const item of input) {
    const durable = DurableRuntimeMessageSchema.safeParse(item);
    const parsed = durable.success ? durable : RuntimeMessageSchema.safeParse(item);
    if (!parsed.success) {
      return { ok: false, error: runtimeInputError("schema") };
    }
    parsedMessages.push(parsed.data);
  }
  const toolTracker: ToolProjectionTracker = {
    ownerByModelCallId: new Map(),
    modelCallIdByOwner: new Map(),
    settledModelCallIds: new Set(),
  };
  const context: ProviderContextEntry[] = [];
  for (const message of parsedMessages) {
    if (message.role !== "user" && message.role !== "assistant") {
      return { ok: false, error: runtimeInputError("unsupported_role") };
    }
    if (message.status === "streaming") {
      continue;
    }
    const projected = projectRuntimeMessage(message, toolTracker);
    if (!projected.ok) {
      return { ok: false, error: runtimeInputError(projected.reason) };
    }
    if (projected.entry.content.length > 0) {
      context.push(projected.entry);
    }
  }
  return { ok: true, context };
}

type ProjectionResult =
  | { readonly ok: true; readonly entry: ProviderContextEntry }
  | { readonly ok: false; readonly reason: string };

function projectRuntimeMessage(
  message: RuntimeMessage,
  toolTracker: ToolProjectionTracker,
): ProjectionResult {
  const content: ProviderContextItem[] = [];
  for (const part of [...message.parts].sort((left, right) => left.sequence - right.sequence)) {
    const projected = projectRuntimeContent(message, part, toolTracker);
    if (!projected.ok) {
      return projected;
    }
    content.push(...projected.content);
  }
  return {
    ok: true,
    entry: {
      role: message.role === "user"
        ? ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER
        : ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
      content,
    },
  };
}

type ContentProjectionResult =
  | { readonly ok: true; readonly content: readonly ProviderContextItem[] }
  | { readonly ok: false; readonly reason: string };

function projectRuntimeContent(
  message: RuntimeMessage,
  part: RuntimePart,
  toolTracker: ToolProjectionTracker,
): ContentProjectionResult {
  switch (part.type) {
    case "text":
      return part.text.length === 0
        ? { ok: true, content: [] }
        : { ok: true, content: [{ text: { text: part.text } }] };
    case "reasoning":
      if (message.role !== "assistant") {
        return { ok: false, reason: "unsupported_content" };
      }
      if (part.text.length === 0 && !hasProviderMetadata(part.providerMetadata)) {
        return { ok: true, content: [] };
      }
      return {
        ok: true,
        content: [{
          reasoning: {
            text: part.text,
            metadataJson: JSON.stringify(part.providerMetadata ?? {}),
          },
        }],
      };
    case "tool":
      if (message.role !== "assistant") {
        return { ok: false, reason: "unsupported_content" };
      }
      return projectToolContent(part, toolTracker);
    case "step-start":
    case "step-finish":
      return { ok: true, content: [] };
    default: {
      const unsupportedPart: never = part;
      return unsupportedPart;
    }
  }
}

function hasProviderMetadata(metadata: Extract<RuntimePart, { readonly type: "reasoning" }>["providerMetadata"]): boolean {
  return metadata !== undefined && Object.keys(metadata).length > 0;
}

interface ToolProjectionTracker {
  readonly ownerByModelCallId: Map<string, string>;
  readonly modelCallIdByOwner: Map<string, string>;
  readonly settledModelCallIds: Set<string>;
}

function projectToolContent(
  part: Extract<RuntimePart, { readonly type: "tool" }>,
  tracker: ToolProjectionTracker,
): ContentProjectionResult {
  const unsettled = part.state.status === "pending" || part.state.status === "running";
  const internalRepair = part.state.status === "error" && isInternalProviderToolCall(part);
  const durableOwner = part.toolUseEventId !== undefined && part.toolUseEventId.length > 0;
  if (!unsettled && !internalRepair && !durableOwner) {
    return { ok: false, reason: "missing_tool_use_event_id" };
  }
  const owner = durableOwner ? `runtime:${part.toolUseEventId}` : `internal:${part.toolCallId}`;
  const existingOwner = tracker.ownerByModelCallId.get(part.toolCallId);
  const existingModelCallId = tracker.modelCallIdByOwner.get(owner);
  if (
    (existingOwner !== undefined && existingOwner !== owner) ||
    (existingModelCallId !== undefined && existingModelCallId !== part.toolCallId)
  ) {
    return { ok: false, reason: "conflicting_tool_call_pair" };
  }
  if (unsettled && tracker.settledModelCallIds.has(part.toolCallId)) {
    return { ok: false, reason: "settled_tool_reopened" };
  }
  const firstCall = existingOwner === undefined;
  if (firstCall) {
    tracker.ownerByModelCallId.set(part.toolCallId, owner);
    tracker.modelCallIdByOwner.set(owner, part.toolCallId);
  }
  const toolCall: ProviderContextItem = {
    toolCall: {
      modelToolCallId: part.toolCallId,
      name: part.toolName,
      inputJson: toolInputJson(part),
    },
  };
  if (unsettled) {
    return { ok: true, content: firstCall ? [toolCall] : [] };
  }
  if (tracker.settledModelCallIds.has(part.toolCallId)) {
    return { ok: false, reason: "duplicate_tool_result" };
  }
  tracker.settledModelCallIds.add(part.toolCallId);
  const toolResult: ProviderContextItem = {
    toolResult: {
      modelToolCallId: part.toolCallId,
      ...toolResultOutcome(part),
    },
  };
  return {
    ok: true,
    content: firstCall ? [toolCall, toolResult] : [toolResult],
  };
}

function isInternalProviderToolCall(part: Extract<RuntimePart, { readonly type: "tool" }>): boolean {
  return part.toolUseEventId === undefined && part.toolEvent === undefined;
}

function toolInputJson(part: Extract<RuntimePart, { readonly type: "tool" }>): string {
  if ("input" in part.state && part.state.input !== undefined) {
    return JSON.stringify(part.state.input.value);
  }
  return "{}";
}

function toolResultOutcome(
  part: Extract<RuntimePart, { readonly type: "tool" }>,
): Pick<ProviderToolResult, "completed" | "error" | "cancelled"> {
  switch (part.state.status) {
    case "completed":
      return { completed: { outputJson: JSON.stringify({ text: part.state.output.text }) } };
    case "error":
      return { error: { errorJson: JSON.stringify({ error: part.state.error }) } };
    case "cancelled":
      return { cancelled: {} };
    case "pending":
    case "running":
      throw new Error("unsettled Tool cannot produce a provider Tool Result");
  }
}

function runtimeInputError(reason: string): ProviderError {
  return normalizeProviderError({
    code: "provider_invalid_request",
    retryable: false,
    message: `Runtime context is not valid for Gateway ProviderRequest: ${reason}.`,
  });
}
