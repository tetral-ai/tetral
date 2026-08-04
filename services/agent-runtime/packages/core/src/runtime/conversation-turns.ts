/**
 * @packageDocumentation
 * Partitions Runtime history into complete user-led turns and reports compactability
 * metadata. It guards user-boundary, compaction-checkpoint, task-notification, and
 * internal-repair classification; fork selection uses the user-led boundaries so it
 * never splits a tool exchange, while compaction does not use this partition.
 * ToolRunner calls these helpers when preparing a child thread, and they inspect committed
 * Runtime messages without calling storage or provider services.
 */
import type { RuntimeMessage, RuntimePart } from "../contracts/runtime.js";

/** One complete conversation turn with fork-boundary and compactability classification. */
export interface RuntimeConversationTurn {
  readonly messages: readonly RuntimeMessage[];
  readonly userLed: boolean;
  readonly compactable: boolean;
}

// fork_turns unit: this partition splits history into complete user-led
// conversation turns. A turn opens at a user-message boundary and absorbs every
// assistant/tool record up to the next boundary. Boundaries are read from the
// RuntimeMessage role/origin discriminator:
//
// | RuntimeMessage                                  | opens a turn boundary?  | userLed |
// | ----------------------------------------------- | ----------------------- | ------- |
// | role=user, not a compaction checkpoint          | yes                     | yes     |
// | role=user, origin=runtime compaction checkpoint | no (folds into current) | no      |
// | role=user, origin=runtime task notification     | yes                     | yes     |
// | role=assistant / any non-user                   | no (extends current)    | n/a     |
//
// The single origin=runtime exception is the compaction checkpoint; a runtime
// task-notification message (role=user, origin=runtime, non-checkpoint) DOES open
// a boundary and counts as a user-led turn. Compaction deliberately does NOT use
// this unit — it selects `recent` by an 8,000-token budget that may split an entry
// mid-entry. `compactable` separately classifies user-led turns containing neither a
// checkpoint nor an internal tool-repair record; selectRecentUserLedTurns does not
// consult that field.
// UPDATE-WITH: services/agent-runtime/packages/core/src/thread-loop/thread-loop.ts
/** Partitions committed history at user-led boundaries without splitting tool exchanges. */
export function partitionRuntimeConversationTurns(
  messages: readonly RuntimeMessage[],
): readonly RuntimeConversationTurn[] {
  const turns: RuntimeMessage[][] = [];
  for (const message of messages) {
    if ((message.role === "user" && !isCompactionCheckpoint(message)) || turns.length === 0) {
      turns.push([message]);
      continue;
    }
    turns[turns.length - 1]?.push(message);
  }
  return turns.map((turn) => ({
    messages: turn,
    userLed: turn[0]?.role === "user" && !isCompactionCheckpoint(turn[0]),
    compactable:
      turn[0]?.role === "user" &&
      !turn.some(isCompactionCheckpoint) &&
      !turn.some(hasInternalToolRepair),
  }));
}

/** Selects the requested number of most recent complete user-led turns. */
export function selectRecentUserLedTurns(
  messages: readonly RuntimeMessage[],
  count: number,
): readonly RuntimeMessage[] {
  return partitionRuntimeConversationTurns(messages)
    .filter((turn) => turn.userLed)
    .slice(-count)
    .flatMap((turn) => turn.messages);
}

function isCompactionCheckpoint(message: RuntimeMessage): boolean {
  return message.role === "user" && message.origin === "runtime" && message.parts.some((part) =>
    part.type === "text" && part.text.includes("<conversation-checkpoint>")
  );
}

function hasInternalToolRepair(message: RuntimeMessage): boolean {
  return message.role === "assistant" &&
    message.origin === "agent" &&
    message.parts.some((part): part is Extract<RuntimePart, { readonly type: "tool" }> =>
      part.type === "tool" &&
      part.toolUseEventId === undefined &&
      part.state.status === "error"
    );
}
