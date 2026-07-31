/**
 * This module owns the ordered hot message view and rewrite generation for one
 * Runtime thread. It guards copy-on-read ownership, append-versus-rewrite cursor
 * semantics, and compaction preservation above a sequence boundary. SessionState
 * and AgentLoop call it after write acknowledgements and while hydrating already
 * durable cold state; SessionManager supplies preload data, and this module performs
 * only in-memory message-list transformations.
 *
 * @packageDocumentation
 */

import type { RuntimeMessage } from "../contracts/runtime.js";

/** Durable, non-sequenced parent context installed separately from child history. */
export interface ThreadContextPrefix {
  readonly childThreadId: string;
  readonly parentThreadId: string;
  readonly parentBoundaryEventId: string;
  readonly entries: readonly RuntimeMessage[];
  readonly createdAt: string;
  readonly consumedByCheckpointMessageId?: string | undefined;
}

// ContextManager owns the hot RuntimeMessage list for one ThreadEntry. New writes are
// projected only after durable ACK; cold hydration may append projections already read
// from durable state. These methods carry the hot mutation, but the discipline
// permitting each one lives at the AgentLoop/SessionManager call site:
//
// | hot mutation                       | gated on durable ACK            |
// | ---------------------------------- | ------------------------------- |
// | append model-visible message input | CommitInputs ACK                |
// | append agent event content         | WriteEvent ACK                  |
// | replace terminal assistant message | WriteRequestEnd ACK             |
// | apply compaction (prefix replace)  | compaction event/projection ACK |
//
// SessionState separately owns the last-request usage hint and route-effective
// limits. A terminal RuntimeMessage may also carry its own usage when the
// WriteRequestEnd-gated projection replaces that message in this list.
//
// A tool-confirmation commit appends its database-stamped user decision before
// waking the approved tool. A task notification uses the background-task settlement
// path and projects a bounded runtime note. A thread context prefix is loaded from the CHILD
// thread's durable context and is never rebuilt from the current parent. The
// generation counter advances on any non-append-only rewrite (compaction or in-place
// update) so the approval-reviewer feed cursor can detect invalidation within one hot
// lifetime. A cold return creates a successor reviewer manager and trunk, which starts
// with a full feed instead of inheriting this generation.
// UPDATE-WITH: services/agent-runtime/packages/core/src/agent-loop/agent-loop.ts,
//              services/agent-runtime/packages/core/src/session/session-manager.ts
/** Mutable hot message list whose generation invalidates reviewer feed cursors on rewrites. */
export class ContextManager {
  readonly sessionId: string;
  #messages: RuntimeMessage[];
  #threadContextPrefix: ThreadContextPrefix | undefined;
  #generation = 0;

  constructor(sessionId: string, initialMessages: readonly RuntimeMessage[] = []) {
    this.sessionId = sessionId;
    this.#messages = [...initialMessages];
  }

  messages(): readonly RuntimeMessage[] {
    return [...this.#messages];
  }

  threadContextPrefix(): ThreadContextPrefix | undefined {
    return this.#threadContextPrefix;
  }

  installThreadContextPrefix(prefix: ThreadContextPrefix | undefined): void {
    this.#threadContextPrefix = prefix;
  }

  providerMessages(): readonly RuntimeMessage[] {
    return [...(this.#threadContextPrefix?.entries ?? []), ...this.#messages];
  }

  messageListSnapshot(): { readonly generation: number; readonly messages: readonly RuntimeMessage[] } {
    return {
      generation: this.#generation,
      messages: this.messages(),
    };
  }

  replaceMessages(messages: readonly RuntimeMessage[]): void {
    const appendOnly = this.#messages.length <= messages.length && this.#messages.every(
      (message, index) => JSON.stringify(message) === JSON.stringify(messages[index]),
    );
    this.#messages = [...messages];
    if (!appendOnly) {
      this.#generation += 1;
    }
  }

  replaceMessagesThroughSequence(boundarySequence: number, messages: readonly RuntimeMessage[]): readonly RuntimeMessage[] {
    const preserved = this.#messages.filter((message) => message.sequence > boundarySequence);
    this.#messages = [...messages, ...preserved];
    this.#generation += 1;
    return this.messages();
  }

  appendMessage(message: RuntimeMessage): void {
    this.#messages = [...this.#messages, message];
  }

  message(messageId: string): RuntimeMessage | undefined {
    return this.#messages.find((message) => message.id === messageId);
  }

  updateMessage(message: RuntimeMessage): void {
    this.#messages = this.#messages.map((existingMessage) => (existingMessage.id === message.id ? message : existingMessage));
    this.#generation += 1;
  }

  clear(): void {
    this.#messages = [];
    this.#threadContextPrefix = undefined;
    this.#generation += 1;
  }
}
