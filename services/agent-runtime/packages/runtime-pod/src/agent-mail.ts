/** Projects one Bridge-owned bounded mail body into the current Runtime input shape. */

import { RuntimeMessageSchema } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import type { RuntimeMessage } from "@tetral/agent-runtime-core/src/contracts/runtime.js";

export function runtimeMessageFromAgentMailContent(input: {
  readonly sessionId: string;
  readonly deliveryId: string;
  readonly content: string;
}): RuntimeMessage {
  const messageId = `agent_mail:${input.deliveryId}`;
  const representationTime = "1970-01-01T00:00:00.000Z";
  return RuntimeMessageSchema.parse({
    id: messageId,
    sessionId: input.sessionId,
    role: "user",
    origin: "agent",
    sequence: 0,
    status: "completed",
    createdAt: representationTime,
    parts: [{
      id: `agent_mail_part:${input.deliveryId}`,
      sessionId: input.sessionId,
      messageId,
      sequence: 0,
      type: "text",
      text: input.content,
      truncated: false,
      status: "completed",
      createdAt: representationTime,
      completedAt: representationTime,
    }],
  });
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
