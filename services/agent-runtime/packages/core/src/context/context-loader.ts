/**
 * @packageDocumentation
 * Defines the interface through which AgentLoop hydrates committed thread context and accepted
 * input, plus a defensive adapter for the basic context and pending-input reads.
 * ContextLoaderBoundary guards ownership, count, byte, schema, and known unsafe-payload checks on
 * its two reads and repairs incomplete persisted parts during buildContext. AgentLoop consumes
 * the host-supplied interface; the boundary calls ContextLoaderSource, validation helpers, and
 * the supplied RuntimeMessageStore.
 */
import { Context, Layer } from "effect";
import type { ProviderRequestAttachment } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type {
  PendingInputResult,
  RuntimeJsonValue,
  RuntimeMessage,
  RuntimeMessageInfo,
  RuntimeMessageStore,
  RuntimeMessageStoreOperationControls,
  RuntimePart,
} from "../contracts/runtime.js";
import type { RuntimeAcceptedInputState, RuntimeAcceptedThreadMetadataState, RuntimePreloadedBackgroundToolState, RuntimeThreadControlState } from "../session/session-state.js";
import type { RuntimeConfigPatchState } from "../session/session-state.js";
import type { RuntimeSessionIdentity } from "../session/session.js";
import {
  ContextLoaderErrorSchema,
  PendingInputResultSchema,
  RuntimeMessageSchema,
  RuntimePartSchema,
  normalizeContextLoaderError,
} from "../contracts/runtime.js";
import {
  containsUnsafeContextPayload,
  contextLoaderFailure,
  validateRuntimeMessageForContext,
} from "./context-validation.js";

type TextOrReasoningRuntimePart = Extract<RuntimePart, { readonly type: "text" | "reasoning" }>;

/** Fixed defensive limits applied to context returned by an external loader source. */
export const ContextLoaderBounds = {
  maxLoadedMessages: 500,
  maxPendingMessages: 20,
  maxMessageParts: 2_000,
  maxPartBytes: 256_000,
} as const;

/** Operations through which session orchestration loads and commits thread context. */
export interface ContextLoader {
  /**
   * Persistent context read boundary: returns hydrated RuntimeMessage history,
   * repairs incomplete assistant/tool state before return, and does not own event-table reads.
   */
  readonly buildContext: (sessionId: string) => Promise<readonly RuntimeMessage[]>;
  readonly buildThreadContext?: (identity: RuntimeSessionIdentity) => Promise<readonly RuntimeMessage[]>;
  /**
   * Cold thread-context read: reconstructs the hydrated ThreadEntry view — history
   * messages, runtime binding token, runtime config patch, mcpManifests (per server:
   * tools, etag, generation, readiness), the pendingToolUses wait set, background
   * tool handles, and pending attachments.
   *
   * Install ordering (enforced by the consumer, not decided here): the latest
   * accepted MCP manifest per server MUST be installed into the tool registry BEFORE
   * the pendingToolUses wait state, so a cold-resumed approved MCP call always finds
   * its ToolEntry. An unready server carries no tools and may carry no etag; it
   * installs zero tools and never aborts cold start.
   * UPDATE-WITH: services/agent-runtime/packages/core/src/session/session-manager.ts,
   *              services/agent-runtime/packages/core/src/agent-loop/agent-loop.ts
   */
  readonly loadThreadContext?: (
    command: RuntimeThreadControlState,
    options?: { readonly agentMailSourceThreadId?: string | undefined },
  ) => Promise<{
    readonly messages: readonly RuntimeMessage[];
    readonly runtimeBindingToken: string;
    readonly thread?: RuntimeAcceptedThreadMetadataState | undefined;
    readonly runtimeConfigPatch?: RuntimeConfigPatchState | undefined;
    readonly mcpManifests?: readonly RuntimeConfigPatchState[] | undefined;
    readonly pendingToolUses?: readonly RuntimeLoadedPendingToolUse[] | undefined;
    readonly backgroundTools?: readonly RuntimePreloadedBackgroundToolState[] | undefined;
    readonly pendingAttachments?: readonly ProviderRequestAttachment[] | undefined;
    readonly pendingAgentMail?: readonly RuntimeLoadedAgentMail[] | undefined;
  }>;
  readonly refreshRuntimeBindingToken?: (
    identity: RuntimeSessionIdentity,
    options?: { readonly force?: boolean | undefined },
  ) => Promise<string>;
  /**
   * Pending event result boundary: Bridge has classified the result; Agent Pod
   * validates it and does not classify raw rows here.
   */
  readonly loadPendingInput: (sessionId: string) => Promise<PendingInputResult>;
  /**
   * Accepted input commit boundary: persists the Runtime command snapshot through
   * Bridge before ThreadRun removes or applies the payload to hot context. ThreadRun
   * may install an in-flight commit marker first so interrupt fencing observes it.
   */
  readonly commitAcceptedInput?: (
    input: RuntimeAcceptedInputState,
    options?: {
      readonly registerScope?: boolean | undefined;
      readonly hydrateContext?: boolean | undefined;
    },
  ) => Promise<AcceptedInputCommitResult>;
}

/** Result of committing one accepted command before mutating its hot thread state. */
export type AcceptedInputCommitResult =
  | PendingInputResult
  | {
      readonly type: "receipt";
      readonly inputDisposition: "committed" | "duplicate";
    }
  | {
      readonly type: "context";
      readonly messages: readonly RuntimeMessage[];
      readonly runtimeBindingToken: string;
      readonly thread?: RuntimeAcceptedThreadMetadataState | undefined;
      readonly runtimeConfigPatch?: RuntimeConfigPatchState | undefined;
      readonly mcpManifests?: readonly RuntimeConfigPatchState[] | undefined;
      readonly pendingToolUses?: readonly RuntimeLoadedPendingToolUse[] | undefined;
      readonly backgroundTools?: readonly RuntimePreloadedBackgroundToolState[] | undefined;
      readonly pendingAttachments?: readonly ProviderRequestAttachment[] | undefined;
      readonly pendingAgentMail?: readonly RuntimeLoadedAgentMail[] | undefined;
      readonly inputDisposition?: "committed" | "duplicate" | undefined;
    };

/** Durable pending tool-use state reconstructed during a cold thread load. */
export interface RuntimeLoadedPendingToolUse {
  readonly toolUseEventId: string;
  readonly modelRequestId: string;
  readonly modelToolCallId: string;
  readonly toolName: string;
  readonly kind: "approval" | "custom";
  readonly input: RuntimeJsonValue;
  readonly decision?: "allow" | "deny" | undefined;
  readonly denyMessage?: string | undefined;
  readonly status: "pending" | "resolving";
  readonly expiresAt: string;
}

/** Unreceipted child completion reconstructed from the durable sent-event ledger. */
export interface RuntimeLoadedAgentMail {
  readonly deliveryId: string;
  readonly sourceThreadId: string;
  readonly sourceToolUseEventId: string;
  readonly message: RuntimeMessage;
}

/** Untrusted external read surface consumed by {@link ContextLoaderBoundary}. */
export interface ContextLoaderSource {
  readonly buildContext: (sessionId: string) => Promise<unknown>;
  readonly loadPendingInput: (sessionId: string) => Promise<unknown>;
}

/** Dependencies used to validate and repair values returned by a context source. */
export interface ContextLoaderBoundaryOptions {
  readonly source: ContextLoaderSource;
  readonly store: RuntimeMessageStore;
  readonly storeControls: () => RuntimeMessageStoreOperationControls;
}

/** Effect service tag through which orchestration obtains the active context loader. */
export class ContextLoaderService extends Context.Service<ContextLoaderService, ContextLoader>()("tetral-agent/ContextLoader") {}

/** Provides a concrete context loader as an Effect layer. */
export function layer(loader: ContextLoader): Layer.Layer<ContextLoaderService> {
  return Layer.succeed(ContextLoaderService, ContextLoaderService.of(loader));
}

/** Validates untrusted loader output and repairs incomplete message state before returning it. */
export class ContextLoaderBoundary implements ContextLoader {
  readonly #source: ContextLoaderSource;
  readonly #store: RuntimeMessageStore;
  readonly #storeControls: () => RuntimeMessageStoreOperationControls;

  constructor(options: ContextLoaderBoundaryOptions) {
    this.#source = options.source;
    this.#store = options.store;
    this.#storeControls = options.storeControls;
  }

  async buildContext(sessionId: string): Promise<readonly RuntimeMessage[]> {
    try {
      const rawMessages = await this.#source.buildContext(sessionId);
      if (containsUnsafeContextPayload(rawMessages)) {
        throw contextLoaderFailure(sessionId, "unsafe_payload", "loader output contains unsafe raw payload");
      }
      if (!Array.isArray(rawMessages)) {
        throw contextLoaderFailure(sessionId, "schema_mismatch", "buildContext must return RuntimeMessage[]");
      }
      if (rawMessages.length > ContextLoaderBounds.maxLoadedMessages) {
        throw contextLoaderFailure(sessionId, "bounds_exceeded", "loaded message count exceeds loader bound");
      }

      const messages: RuntimeMessage[] = [];
      let loadedPartCount = 0;
      for (const rawMessage of rawMessages) {
        const parsed = RuntimeMessageSchema.safeParse(rawMessage);
        if (!parsed.success) {
          throw contextLoaderFailure(sessionId, "schema_mismatch", "loader returned malformed runtime message");
        }
        loadedPartCount += parsed.data.parts.length;
        if (loadedPartCount > ContextLoaderBounds.maxMessageParts) {
          throw contextLoaderFailure(sessionId, "bounds_exceeded", "loaded runtime part count exceeds loader bound");
        }
        const validationError = validateRuntimeMessageForContext(parsed.data, sessionId, ContextLoaderBounds);
        if (validationError !== undefined) {
          throw validationError;
        }
        const repaired = await this.#repairMessage(sessionId, parsed.data);
        if (repaired !== undefined) {
          messages.push(repaired);
        }
      }
      return messages;
    } catch (error) {
      throw this.#normalizeThrownLoaderError(sessionId, error);
    }
  }

  async loadPendingInput(sessionId: string): Promise<PendingInputResult> {
    try {
      const rawPending = await this.#source.loadPendingInput(sessionId);
      if (containsUnsafeContextPayload(rawPending)) {
        throw contextLoaderFailure(sessionId, "unsafe_payload", "pending input contains unsafe raw payload");
      }
      const parsed = PendingInputResultSchema.safeParse(rawPending);
      if (!parsed.success) {
        throw contextLoaderFailure(sessionId, "schema_mismatch", "pending input is malformed or not user-only");
      }
      if (parsed.data.type === "messages") {
        if (parsed.data.messages.length > ContextLoaderBounds.maxPendingMessages) {
          throw contextLoaderFailure(sessionId, "bounds_exceeded", "pending message count exceeds loader bound");
        }
        let pendingPartCount = 0;
        for (const message of parsed.data.messages) {
          pendingPartCount += message.parts.length;
          if (pendingPartCount > ContextLoaderBounds.maxMessageParts) {
            throw contextLoaderFailure(sessionId, "bounds_exceeded", "pending runtime part count exceeds loader bound");
          }
          const validationError = validateRuntimeMessageForContext(message, sessionId, ContextLoaderBounds);
          if (validationError !== undefined) {
            throw validationError;
          }
        }
      }
      return parsed.data;
    } catch (error) {
      throw this.#normalizeThrownLoaderError(sessionId, error);
    }
  }

  async #repairMessage(sessionId: string, message: RuntimeMessage): Promise<RuntimeMessage | undefined> {
    if (message.role !== "assistant") {
      return message;
    }
    if (message.status === "failed" && message.error?.reason !== "aborted") {
      return undefined;
    }
    const repairedParts: RuntimePart[] = [];
    let changed = false;
    let hasVisibleContent = false;
    for (const part of message.parts) {
      if (part.type === "text" || part.type === "reasoning") {
        const repairedPart = await this.#repairTextOrReasoningPart(part);
        if (repairedPart !== part) {
          changed = true;
        }
        hasVisibleContent ||= repairedPart.text.trim().length > 0;
        repairedParts.push(repairedPart);
        continue;
      }
      if (part.type === "tool" && (part.state.status === "pending" || part.state.status === "running")) {
        const repairedPart = RuntimePartSchema.parse({
          ...part,
          state: {
            status: "cancelled",
            ...(part.state.status === "running" ? { input: part.state.input } : {}),
          },
        });
        const writeResult = await this.#store.writePart(repairedPart, this.#storeControls());
        if (!writeResult.ok) {
          throw normalizeContextLoaderError({
            code: "unavailable",
            sessionId,
            reason: "cold repair write-through failed",
          });
        }
        repairedParts.push(repairedPart);
        changed = true;
        hasVisibleContent = true;
        continue;
      }
      if (part.type === "tool") {
        repairedParts.push(part);
        hasVisibleContent = true;
      }
    }
    const repairedMessage = await this.#repairAssistantMessageInfo(message, changed);
    if (!hasVisibleContent) {
      return undefined;
    }
    return RuntimeMessageSchema.parse({ ...repairedMessage, parts: repairedParts });
  }

  async #repairTextOrReasoningPart(part: TextOrReasoningRuntimePart): Promise<TextOrReasoningRuntimePart> {
    if (part.status !== "streaming") {
      return part;
    }
    const repairedPart = parseTextOrReasoningRuntimePart({
      ...part,
      status: "cancelled",
      updatedAt: part.updatedAt ?? part.createdAt,
      completedAt: part.updatedAt ?? part.createdAt,
    });
    const writeResult = await this.#store.writePart(repairedPart, this.#storeControls());
    if (!writeResult.ok) {
      throw normalizeContextLoaderError({
        code: "unavailable",
        sessionId: part.sessionId,
        reason: "cold repair write-through failed",
      });
    }
    return repairedPart;
  }

  async #repairAssistantMessageInfo(message: RuntimeMessage, partsChanged: boolean): Promise<RuntimeMessage> {
    if (message.status !== "streaming" && !partsChanged) {
      return message;
    }
    const repairedMessage = RuntimeMessageSchema.parse({
      ...message,
      status: "cancelled",
      updatedAt: message.updatedAt ?? message.createdAt,
    });
    const writeResult = await this.#store.writeMessage(runtimeMessageInfo(repairedMessage), this.#storeControls());
    if (!writeResult.ok) {
      throw normalizeContextLoaderError({
        code: "unavailable",
        sessionId: message.sessionId,
        reason: "cold repair write-through failed",
      });
    }
    return repairedMessage;
  }

  #normalizeThrownLoaderError(sessionId: string, error: unknown): unknown {
    const parsed = ContextLoaderErrorSchema.safeParse(error);
    if (parsed.success) {
      return parsed.data;
    }
    return normalizeContextLoaderError({ code: "unknown", rawError: error, sessionId });
  }
}

function runtimeMessageInfo(message: RuntimeMessage): RuntimeMessageInfo {
  return {
    id: message.id,
    sessionId: message.sessionId,
    role: message.role,
    origin: message.origin,
    sequence: message.sequence,
    status: message.status,
    createdAt: message.createdAt,
    ...(message.updatedAt !== undefined ? { updatedAt: message.updatedAt } : {}),
    ...(message.error !== undefined ? { error: message.error } : {}),
    ...(message.finishReason !== undefined ? { finishReason: message.finishReason } : {}),
    ...(message.usage !== undefined ? { usage: message.usage } : {}),
    ...(message.providerId !== undefined ? { providerId: message.providerId } : {}),
    ...(message.modelId !== undefined ? { modelId: message.modelId } : {}),
    ...(message.responseId !== undefined ? { responseId: message.responseId } : {}),
  };
}

function parseTextOrReasoningRuntimePart(input: unknown): TextOrReasoningRuntimePart {
  const part = RuntimePartSchema.parse(input);
  if (part.type !== "text" && part.type !== "reasoning") {
    throw new Error("expected text or reasoning runtime part");
  }
  return part;
}
