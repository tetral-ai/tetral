/**
 * @packageDocumentation
 * Projects the durable RuntimeMessage view that a provider request is assembled
 * from.
 *
 * OWNS:
 *   - toGatewayRuntimeMessages: the RuntimeMessage -> Gateway RuntimeMessage lowering.
 *
 * STATE MACHINE (per message, at lowering time):
 *   status=streaming                 -> skipped, never lowered.
 *   status in {completed,failed,cancelled} -> lowered; empty text and empty
 *                                        reasoning parts drop out, and a message
 *                                        that lowers to zero parts is omitted.
 *   tool part status pending|running -> skipped only when a later message carries
 *                                        the same tool-use identity in a terminal
 *                                        state; otherwise rejected. Internal
 *                                        provider tool calls lower to nothing.
 *
 * INVARIANTS:
 *   - Only non-streaming messages are projected. Text and reasoning part status is
 *     not rechecked here; unresolved pending/running tool state is rejected, while
 *     Provider deltas and Gateway raw stream chunks never reach this input.
 *     Message usage may be present but is ignored by lowering.
 *   - The sole retained provider-native field is a reasoning part's
 *     metadata, kept for reasoning round-trip; it is internal cold-start context and
 *     never surfaces on any public event or message API.
 * UPDATE-WITH: services/agent-runtime/packages/core/src/contracts/runtime.ts,
 *              services/agent-runtime/packages/core/src/runtime/accumulator.ts
 *
 * AgentLoop calls toGatewayRuntimeMessages while assembling provider requests.
 * Lowering calls the generated Gateway message protocol shapes for provider
 * request assembly.
 */
import {
  RuntimeMessageRole,
  RuntimeToolPartState,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type {
  RuntimeMessage as GatewayRuntimeMessage,
  RuntimePart as GatewayRuntimePart,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { RuntimeMessage, RuntimePart } from "../contracts/runtime.js";
import type { ProviderError } from "../contracts/provider.js";
import {
  DurableRuntimeMessageSchema,
  RuntimeMessageSchema,
} from "../contracts/runtime.js";
import { normalizeProviderError } from "../contracts/provider.js";

/** Success or normalized provider-input failure from lowering Runtime messages. */
export type RuntimeGatewayMessagesResult =
  | { readonly ok: true; readonly messages: readonly GatewayRuntimeMessage[] }
  | { readonly ok: false; readonly error: ProviderError };

/** Lowers completed, failed, and cancelled Runtime history into provider-request messages. */
export function toGatewayRuntimeMessages(input: unknown): RuntimeGatewayMessagesResult {
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
  const terminalToolMessageIndex = new Map<string, number>();
  for (const [messageIndex, message] of parsedMessages.entries()) {
    for (const part of message.parts) {
      if (
        part.type === "tool" &&
        part.toolUseEventId !== undefined &&
        part.state.status !== "pending" &&
        part.state.status !== "running"
      ) {
        terminalToolMessageIndex.set(part.toolUseEventId, messageIndex);
      }
    }
  }
  const messages: GatewayRuntimeMessage[] = [];
  for (const [messageIndex, message] of parsedMessages.entries()) {
    if (message.role !== "user" && message.role !== "assistant") {
      return { ok: false, error: runtimeInputError("unsupported_role") };
    }
    if (message.status === "streaming") {
      continue;
    }
    const projected = projectRuntimeMessage(message, messageIndex, terminalToolMessageIndex);
    if (!projected.ok) {
      return { ok: false, error: runtimeInputError(projected.reason) };
    }
    if (projected.message.parts.length > 0) {
      messages.push(projected.message);
    }
  }
  return { ok: true, messages };
}

type ProjectionResult =
  | { readonly ok: true; readonly message: GatewayRuntimeMessage }
  | { readonly ok: false; readonly reason: string };

function projectRuntimeMessage(
  message: RuntimeMessage,
  messageIndex: number,
  terminalToolMessageIndex: ReadonlyMap<string, number>,
): ProjectionResult {
  const parts: GatewayRuntimePart[] = [];
  for (const part of [...message.parts].sort((left, right) => left.sequence - right.sequence)) {
    if (
      part.type === "tool" &&
      (part.state.status === "pending" || part.state.status === "running") &&
      part.toolUseEventId !== undefined &&
      (terminalToolMessageIndex.get(part.toolUseEventId) ?? -1) > messageIndex
    ) {
      continue;
    }
    const projected = projectRuntimePart(message, part);
    if (!projected.ok) {
      return projected;
    }
    if (projected.part !== undefined) {
      parts.push(projected.part);
    }
  }
  return {
    ok: true,
    message: {
      id: message.id,
      role: message.role === "user" ? RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_USER : RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_ASSISTANT,
      parts,
      status: message.status,
      origin: message.origin,
    },
  };
}

type PartProjectionResult =
  | { readonly ok: true; readonly part?: GatewayRuntimePart | undefined }
  | { readonly ok: false; readonly reason: string };

function projectRuntimePart(message: RuntimeMessage, part: RuntimePart): PartProjectionResult {
  switch (part.type) {
    case "text":
      if (part.text.length === 0) {
        return { ok: true };
      }
      return {
        ok: true,
        part: {
          id: part.id,
          text: { text: part.text },
        },
      };
    case "reasoning":
      if (message.role !== "assistant") {
        return { ok: false, reason: "unsupported_content" };
      }
      if (part.text.length === 0 && !hasProviderMetadata(part.providerMetadata)) {
        return { ok: true };
      }
      return {
        ok: true,
        part: {
          id: part.id,
          reasoning: {
            text: part.text,
            metadataJson: providerMetadataJson(part.providerMetadata),
          },
        },
      };
    case "tool":
      if (message.role !== "assistant") {
        return { ok: false, reason: "unsupported_content" };
      }
      return projectToolPart(part);
    case "step-start":
    case "step-finish":
      return { ok: true };
    default: {
      const unsupportedPart: never = part;
      return unsupportedPart;
    }
  }
}

function providerMetadataJson(metadata: Extract<RuntimePart, { readonly type: "reasoning" }>["providerMetadata"]): string {
  return JSON.stringify(metadata ?? {});
}

function hasProviderMetadata(metadata: Extract<RuntimePart, { readonly type: "reasoning" }>["providerMetadata"]): boolean {
  return metadata !== undefined && Object.keys(metadata).length > 0;
}

function projectToolPart(part: Extract<RuntimePart, { readonly type: "tool" }>): PartProjectionResult {
  if (part.state.status === "pending" || part.state.status === "running") {
    if (isInternalProviderToolCall(part)) {
      return { ok: true };
    }
    return { ok: false, reason: "pending_tool_state" };
  }
  const internalRepair = part.state.status === "error" &&
    isInternalProviderToolCall(part);
  if (!internalRepair && (part.toolUseEventId === undefined || part.toolUseEventId.length === 0)) {
    return { ok: false, reason: "missing_tool_use_event_id" };
  }
  return {
    ok: true,
    part: {
      id: part.id,
      tool: {
        callId: part.toolCallId,
        name: part.toolName,
        toolUseEventId: part.toolUseEventId,
        state: gatewayToolState(part.state.status),
        inputJson: toolInputJson(part),
        outputOrErrorJson: toolOutputOrErrorJson(part),
      },
    },
  };
}

function isInternalProviderToolCall(part: Extract<RuntimePart, { readonly type: "tool" }>): boolean {
  return part.toolUseEventId === undefined && part.toolEvent === undefined;
}

function gatewayToolState(status: Exclude<Extract<RuntimePart, { readonly type: "tool" }>["state"]["status"], "pending" | "running">): RuntimeToolPartState {
  switch (status) {
    case "completed":
      return RuntimeToolPartState.RUNTIME_TOOL_PART_STATE_COMPLETED;
    case "error":
      return RuntimeToolPartState.RUNTIME_TOOL_PART_STATE_ERROR;
    case "cancelled":
      return RuntimeToolPartState.RUNTIME_TOOL_PART_STATE_CANCELLED;
  }
}

function toolInputJson(part: Extract<RuntimePart, { readonly type: "tool" }>): string {
  if ("input" in part.state && part.state.input !== undefined) {
    return JSON.stringify(part.state.input.value ?? part.state.input.preview);
  }
  return "{}";
}

function toolOutputOrErrorJson(part: Extract<RuntimePart, { readonly type: "tool" }>): string {
  switch (part.state.status) {
    case "completed":
      return JSON.stringify({ text: part.state.output.text });
    case "error":
      return JSON.stringify({ error: part.state.error });
    case "cancelled":
      return JSON.stringify({ error: part.state.error ?? { type: "cancelled", message: "tool call was cancelled" } });
    case "pending":
    case "running":
      return "{}";
  }
}

function runtimeInputError(reason: string): ProviderError {
  return normalizeProviderError({
    code: "provider_invalid_request",
    retryable: false,
    message: `Runtime context is not valid for Gateway ProviderRequest: ${reason}.`,
  });
}
