import { describe, expect, test } from "bun:test";
import { DurableRuntimeMessageSchema } from "../../../src/contracts/runtime.js";
import type { DurableRuntimeMessage } from "../../../src/contracts/runtime.js";
import {
  extractColdThreadToolRouteView,
  extractThreadTurnCheckpoint,
  ThreadTurnCheckpointSchema,
} from "../../../src/thread-loop/thread-turn-checkpoint.js";
import {
  initializeThreadTurnReduction,
  reduceThreadTurn,
} from "../../../src/thread-loop/thread-turn-reducer.js";

const timestamp = "2026-08-12T00:00:00.000Z";

function textMessage(input: {
  readonly id: string;
  readonly sequence: number;
  readonly role: "user" | "assistant";
  readonly text?: string;
}): DurableRuntimeMessage {
  return DurableRuntimeMessageSchema.parse({
    id: input.id,
    sessionId: "session",
    role: input.role,
    origin: input.role === "user" ? "user" : "agent",
    sequence: input.sequence,
    status: "completed",
    createdAt: timestamp,
    updatedAt: timestamp,
    parts: [{
      id: `${input.id}_part`,
      sessionId: "session",
      messageId: input.id,
      sequence: 0,
      createdAt: timestamp,
      updatedAt: timestamp,
      type: "text",
      text: input.text ?? input.id,
      truncated: false,
      status: "completed",
    }],
  });
}

function toolMessage(input: {
  readonly id: string;
  readonly sequence: number;
  readonly callId: string;
  readonly toolUseEventId?: string;
  readonly toolName: string;
  readonly status: "running" | "completed" | "error" | "cancelled";
}): DurableRuntimeMessage {
  const terminal = input.status !== "running";
  const state = input.status === "running"
    ? {
        status: "running" as const,
        input: { value: {}, preview: "{}", truncated: false },
      }
    : input.status === "completed"
      ? {
          status: "completed" as const,
          input: { value: {}, preview: "{}", truncated: false },
          output: { text: "done", truncated: false },
        }
      : input.status === "error"
        ? {
            status: "error" as const,
            error: { type: "tool_error", message: "failed", retryable: false },
          }
        : {
            status: "cancelled" as const,
            error: { type: "tool_cancelled", message: "cancelled", retryable: false },
          };
  return DurableRuntimeMessageSchema.parse({
    id: input.id,
    sessionId: "session",
    role: "assistant",
    origin: "agent",
    sequence: input.sequence,
    status: terminal ? "completed" : "streaming",
    createdAt: timestamp,
    updatedAt: timestamp,
    parts: [{
      id: `${input.id}_part`,
      sessionId: "session",
      messageId: input.id,
      sequence: 0,
      createdAt: timestamp,
      updatedAt: timestamp,
      ...(terminal ? { completedAt: timestamp } : {}),
      type: "tool",
      toolCallId: input.callId,
      toolName: input.toolName,
      ...(input.toolUseEventId === undefined ? {} : { toolUseEventId: input.toolUseEventId }),
      state,
    }],
  });
}

describe("cold Thread checkpoint direct facts", () => {
  test("rejects a following Request Start until its predecessor is sealed", () => {
    expect(() => extractThreadTurnCheckpoint({
      messages: [],
      facts: {
        events: [
          {
            eventId: "start_first",
            eventSequence: 1,
            type: "span.model_request_start",
            modelRequestId: "request_first",
            requestStart: { requestKind: "agent_provider_request", contextThroughMessageSequence: 0 },
          },
          {
            eventId: "start_second",
            eventSequence: 2,
            type: "span.model_request_start",
            modelRequestId: "request_second",
            requestStart: { requestKind: "agent_provider_request", contextThroughMessageSequence: 0 },
          },
        ],
        internalRepairs: [],
      },
    })).toThrow("following Request Start requires its predecessor to be sealed");
  });

  test("requires-action closeout retains one sealed incomplete Tool Use", () => {
    const checkpoint = ThreadTurnCheckpointSchema.parse({
      pendingInputMessageIds: [],
      request: {
        modelRequestId: "request_approval",
        requestStartEventId: "start_approval",
        requestKind: "agent_provider_request",
        contextThroughMessageSequence: 1,
        requestEnd: { eventId: "end_approval", isError: false, rescheduled: false },
        toolMembers: [{
          memberKind: "public_tool_use",
          modelToolCallId: "call_approval",
          toolUseEventId: "tool_approval",
          toolName: "Write",
        }],
      },
      idleCloseout: { eventId: "idle_approval", stopReason: "requires_action" },
    });

    expect(checkpoint.request?.toolMembers).toHaveLength(1);
    expect(checkpoint.idleCloseout).toEqual({ eventId: "idle_approval", stopReason: "requires_action" });
    expect(() => ThreadTurnCheckpointSchema.parse({
      ...checkpoint,
      request: { ...checkpoint.request!, toolMembers: [] },
    })).toThrow("requires_action closeout must retain a sealed incomplete Tool Use");
  });

  test("rejects a Request Start boundary beyond loaded durable Messages", () => {
    expect(() => extractThreadTurnCheckpoint({
      messages: [textMessage({ id: "only_message", sequence: 1, role: "user" })],
      facts: {
        events: [{
          eventId: "start_out_of_range",
          eventSequence: 1,
          type: "span.model_request_start",
          modelRequestId: "request_out_of_range",
          requestStart: { requestKind: "agent_provider_request", contextThroughMessageSequence: 2 },
        }],
        internalRepairs: [],
      },
    })).toThrow("Request Start message boundary exceeds the loaded durable message range");
  });

  test("rejects a Tool Use ordered after its Request End", () => {
    expect(() => extractThreadTurnCheckpoint({
      messages: [],
      facts: {
        events: [
          {
            eventId: "start_order",
            eventSequence: 1,
            type: "span.model_request_start",
            modelRequestId: "request_order",
            requestStart: { requestKind: "agent_provider_request", contextThroughMessageSequence: 0 },
          },
          {
            eventId: "end_order",
            eventSequence: 2,
            type: "span.model_request_end",
            modelRequestId: "request_order",
            requestEnd: { requestStartEventId: "start_order", isError: false, rescheduled: false },
          },
          {
            eventId: "tool_after_end",
            eventSequence: 3,
            type: "agent.tool_use",
            modelRequestId: "request_order",
            toolUse: {},
          },
        ],
        internalRepairs: [],
      },
    })).toThrow("Tool Use cannot follow Request End");
  });

  test("rejects duplicate Request Start facts for one model request", () => {
    expect(() => extractThreadTurnCheckpoint({
      messages: [],
      facts: {
        events: [1, 2].map((eventSequence) => ({
          eventId: `start_duplicate_${eventSequence}`,
          eventSequence,
          type: "span.model_request_start" as const,
          modelRequestId: "request_duplicate",
          requestStart: { requestKind: "agent_provider_request" as const, contextThroughMessageSequence: 0 },
        })),
        internalRepairs: [],
      },
    })).toThrow("model request has duplicate Request Start facts");
  });

  test("completed text history reaches the reducer without Message mutation lineage", () => {
    const messages = [
      textMessage({ id: "input", sequence: 1, role: "user" }),
      textMessage({ id: "assistant", sequence: 2, role: "assistant" }),
    ];
    const checkpoint = extractThreadTurnCheckpoint({
      messages,
      facts: {
        events: [
          { eventId: "running", eventSequence: 1, type: "session.status_running" },
          {
            eventId: "start",
            eventSequence: 2,
            type: "span.model_request_start",
            modelRequestId: "request",
            requestStart: { requestKind: "agent_provider_request", contextThroughMessageSequence: 1 },
          },
          {
            eventId: "end",
            eventSequence: 3,
            type: "span.model_request_end",
            modelRequestId: "request",
            requestEnd: { requestStartEventId: "start", isError: false, rescheduled: false },
          },
        ],
        internalRepairs: [],
      },
    });

    expect(checkpoint).toEqual({
      executionRunId: "running",
      pendingInputMessageIds: [],
      request: {
        modelRequestId: "request",
        requestStartEventId: "start",
        requestKind: "agent_provider_request",
        contextThroughMessageSequence: 1,
        requestEnd: { eventId: "end", isError: false, rescheduled: false },
        toolMembers: [],
      },
    });
  });

  test("hot receipt facts and cold durable facts select the same next action", () => {
    let hot = initializeThreadTurnReduction({ pendingInputMessageIds: [] }, { routes: [] });
    hot = reduceThreadTurn(hot, { fact: "run_opened", eventId: "running" }, { routes: [] });
    hot = reduceThreadTurn(hot, {
      fact: "inputs_committed",
      eventId: "input_event",
      messageIds: ["input"],
    }, { routes: [] });

    const coldCheckpoint = extractThreadTurnCheckpoint({
      messages: [textMessage({ id: "input", sequence: 1, role: "user" })],
      facts: {
        events: [{ eventId: "running", eventSequence: 1, type: "session.status_running" }],
        internalRepairs: [],
      },
    });
    const cold = initializeThreadTurnReduction(coldCheckpoint, { routes: [] });

    expect({
      checkpoint: cold.checkpoint,
      toolRouteView: { routes: [] },
      reducerAction: cold.action,
    }).toEqual({
      checkpoint: hot.checkpoint,
      toolRouteView: { routes: [] },
      reducerAction: hot.action,
    });
  });

  test("message sequence alone defines pending provider input across mixed origins", () => {
    const messages = [
      textMessage({ id: "before", sequence: 1, role: "user" }),
      textMessage({ id: "at", sequence: 2, role: "user" }),
      textMessage({ id: "assistant", sequence: 3, role: "assistant" }),
      textMessage({ id: "confirmation", sequence: 4, role: "user" }),
      textMessage({ id: "notification", sequence: 5, role: "user" }),
      textMessage({ id: "compaction", sequence: 6, role: "user" }),
    ];
    const checkpoint = extractThreadTurnCheckpoint({
      messages,
      facts: {
        events: [{
          eventId: "start",
          eventSequence: 1,
          type: "span.model_request_start",
          modelRequestId: "request",
          requestStart: { requestKind: "agent_provider_request", contextThroughMessageSequence: 2 },
        }],
        internalRepairs: [],
      },
    });

    expect(checkpoint.pendingInputMessageIds).toEqual(["confirmation", "notification", "compaction"]);
  });

  test("multiple Tool Uses and independently settled results relate through stable Tool identities", () => {
    const messages = [
      toolMessage({
        id: "tool_a_message",
        sequence: 1,
        callId: "call_a",
        toolUseEventId: "tool_a",
        toolName: "Read",
        status: "completed",
      }),
      toolMessage({
        id: "tool_b_message",
        sequence: 2,
        callId: "call_b",
        toolUseEventId: "tool_b",
        toolName: "Grep",
        status: "error",
      }),
    ];
    const checkpoint = extractThreadTurnCheckpoint({
      messages,
      facts: {
        events: [
          {
            eventId: "start",
            eventSequence: 1,
            type: "span.model_request_start",
            modelRequestId: "request",
            requestStart: { requestKind: "agent_provider_request", contextThroughMessageSequence: 0 },
          },
          {
            eventId: "tool_a",
            eventSequence: 2,
            type: "agent.tool_use",
            modelRequestId: "request",
            toolUse: {},
          },
          {
            eventId: "tool_b",
            eventSequence: 3,
            type: "agent.tool_use",
            modelRequestId: "request",
            toolUse: {},
          },
          {
            eventId: "end",
            eventSequence: 4,
            type: "span.model_request_end",
            modelRequestId: "request",
            requestEnd: { requestStartEventId: "start", isError: false, rescheduled: false },
          },
          {
            eventId: "result_a",
            eventSequence: 5,
            type: "agent.tool_result",
            modelRequestId: "request",
            toolResult: { toolUseEventId: "tool_a" },
          },
          {
            eventId: "result_b",
            eventSequence: 6,
            type: "agent.tool_result",
            modelRequestId: "request",
            toolResult: { toolUseEventId: "tool_b" },
          },
        ],
        internalRepairs: [],
      },
    });

    expect(checkpoint.request?.toolMembers).toEqual([
      {
        memberKind: "public_tool_use",
        modelToolCallId: "call_a",
        toolUseEventId: "tool_a",
        toolName: "Read",
        terminalResult: { outcome: "success" },
      },
      {
        memberKind: "public_tool_use",
        modelToolCallId: "call_b",
        toolUseEventId: "tool_b",
        toolName: "Grep",
        terminalResult: { outcome: "error" },
      },
    ]);
  });

  test("internal repair joins its Message and Event through one repair identity", () => {
    const repairKey = "repair_key";
    const checkpoint = extractThreadTurnCheckpoint({
      messages: [toolMessage({
        id: "repair_message",
        sequence: 1,
        callId: "missing_call",
        toolName: "missing_tool",
        status: "error",
      })],
      facts: {
        events: [
          {
            eventId: "start",
            eventSequence: 1,
            type: "span.model_request_start",
            modelRequestId: "request",
            requestStart: { requestKind: "agent_provider_request", contextThroughMessageSequence: 0 },
          },
          {
            eventId: "repair_event",
            eventSequence: 2,
            type: "agent.tool_result",
            modelRequestId: "request",
            toolResult: { repairKey },
          },
        ],
        internalRepairs: [{
          repairKey,
          messageId: "repair_message",
          repairEventId: "repair_event",
          eventSequence: 2,
          modelRequestId: "request",
        }],
      },
    });

    expect(checkpoint.request?.toolMembers).toEqual([{
      memberKind: "internal_tool_repair",
      modelToolCallId: "missing_call",
      toolName: "missing_tool",
      repairEventId: "repair_event",
      outcome: "error",
    }]);
  });

  test("missing targets and conflicting terminal facts fail closed", () => {
    const open = toolMessage({
      id: "tool_message",
      sequence: 1,
      callId: "call",
      toolUseEventId: "tool",
      toolName: "Read",
      status: "running",
    });
    const base = {
      events: [
        {
          eventId: "start",
          eventSequence: 1,
          type: "span.model_request_start" as const,
          modelRequestId: "request",
          requestStart: { requestKind: "agent_provider_request" as const, contextThroughMessageSequence: 0 },
        },
        {
          eventId: "tool",
          eventSequence: 2,
          type: "agent.tool_use" as const,
          modelRequestId: "request",
          toolUse: {},
        },
        {
          eventId: "result",
          eventSequence: 3,
          type: "agent.tool_result" as const,
          modelRequestId: "request",
          toolResult: { toolUseEventId: "tool" },
        },
      ],
      internalRepairs: [],
    };
    expect(() => extractThreadTurnCheckpoint({ messages: [open], facts: base }))
      .toThrow("Tool Result has no matching terminal Message part");
    expect(() => extractThreadTurnCheckpoint({
      messages: [],
      facts: {
        events: base.events.slice(0, 1),
        internalRepairs: [{
          repairKey: "missing",
          messageId: "missing",
          repairEventId: "missing",
          eventSequence: 2,
          modelRequestId: "request",
        }],
      },
    })).toThrow("internal repair direct reference has no matching Event");
    expect(() => extractThreadTurnCheckpoint({
      messages: [],
      facts: {
        events: [
          base.events[0]!,
          {
            eventId: "mcp_repair",
            eventSequence: 2,
            type: "agent.mcp_tool_result",
            modelRequestId: "request",
            toolResult: { repairKey: "repair" },
          },
        ],
        internalRepairs: [],
      },
    })).toThrow("MCP Tool Result cannot be an internal repair");
  });

  test("unfinished Tools keep their independent cold route projection", () => {
    const checkpoint = extractThreadTurnCheckpoint({
      messages: [toolMessage({
        id: "tool_message",
        sequence: 1,
        callId: "call",
        toolUseEventId: "tool",
        toolName: "Read",
        status: "running",
      })],
      facts: {
        events: [
          {
            eventId: "start",
            eventSequence: 1,
            type: "span.model_request_start",
            modelRequestId: "request",
            requestStart: { requestKind: "agent_provider_request", contextThroughMessageSequence: 0 },
          },
          {
            eventId: "tool",
            eventSequence: 2,
            type: "agent.tool_use",
            modelRequestId: "request",
            toolUse: {},
          },
          {
            eventId: "end",
            eventSequence: 3,
            type: "span.model_request_end",
            modelRequestId: "request",
            requestEnd: { requestStartEventId: "start", isError: false, rescheduled: false },
          },
        ],
        internalRepairs: [],
      },
    });
    const view = extractColdThreadToolRouteView({
      checkpoint,
      pendingToolUses: [{
        toolUseEventId: "tool",
        modelRequestId: "request",
        modelToolCallId: "call",
        toolName: "Read",
        status: "pending",
      }],
      pendingSandboxExecutions: [],
    });
    expect(view).toEqual({ routes: [{ toolUseEventId: "tool", disposition: "requires_user_action" }] });
  });
});
