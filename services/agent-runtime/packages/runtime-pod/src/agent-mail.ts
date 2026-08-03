/**
 * Converts the public inter-agent envelope into the smaller Runtime message
 * representation. The durable `parts` are authoritative; `content` is a
 * public projection that must match before it is removed at the command edge.
 */

import { RuntimeMessageSchema } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import type { RuntimeMessage } from "@tetral/agent-runtime-core/src/contracts/runtime.js";

/** Validates public derived content and returns the authoritative Runtime message. */
export function runtimeMessageFromPublicAgentMail(raw: string): RuntimeMessage {
  const parsed: unknown = JSON.parse(raw);
  if (!isRecord(parsed) || !Array.isArray(parsed.content)) {
    throw new Error("agent mail public message is malformed");
  }
  const { content, ...runtimeShape } = parsed;
  const message = RuntimeMessageSchema.parse(runtimeShape);
  const expected = message.parts.map((part) => {
    if (part.type !== "text") {
      throw new Error("agent mail Runtime message contains a non-text part");
    }
    return { type: "text", text: part.text };
  });
  if (
    content.length !== expected.length ||
    content.some((block, index) =>
      !isRecord(block) ||
      Object.keys(block).length !== 2 ||
      block.type !== expected[index]?.type ||
      block.text !== expected[index]?.text
    )
  ) {
    throw new Error("agent mail public content does not match Runtime parts");
  }
  return message;
}

/** Renders the validated text parts returned by `wait_agent`. */
export function runtimeAgentMailText(message: RuntimeMessage): string {
  return message.parts.map((part) => {
    if (part.type !== "text") {
      throw new Error("agent mail Runtime message contains a non-text part");
    }
    return part.text;
  }).join("\n");
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
