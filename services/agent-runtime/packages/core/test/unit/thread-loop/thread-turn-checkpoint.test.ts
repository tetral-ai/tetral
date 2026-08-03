import { describe, expect, test } from "bun:test";
import {
  ThreadToolRouteViewSchema,
  ThreadTurnCheckpointSchema,
} from "../../../src/thread-loop/thread-turn-checkpoint.js";

describe("ThreadTurnCheckpoint", () => {
  test("accepts the closed durable read model", () => {
    expect(ThreadTurnCheckpointSchema.parse({
      executionRunId: "run_1",
      pendingInputMessageIds: ["msg_2"],
      request: {
        modelRequestId: "request_1",
        requestStartEventId: "event_start",
        requestKind: "agent_provider_request",
        contextThroughMessageSequence: 1,
        requestEnd: {
          eventId: "event_end",
          isError: false,
          rescheduled: false,
        },
        toolMembers: [
          {
            memberKind: "public_tool_use",
            modelToolCallId: "call_1",
            toolUseEventId: "event_tool_1",
            toolName: "Read",
          },
          {
            memberKind: "internal_tool_repair",
            modelToolCallId: "call_2",
            toolName: "missing_tool",
            repairEventId: "event_repair_1",
            outcome: "error",
          },
        ],
      },
    })).toMatchObject({
      executionRunId: "run_1",
      pendingInputMessageIds: ["msg_2"],
    });
  });

  test("rejects ambiguous request membership and malformed durable identities", () => {
    const duplicateCallId = {
      pendingInputMessageIds: [],
      request: {
        modelRequestId: "request_1",
        requestStartEventId: "event_start",
        requestKind: "agent_provider_request",
        contextThroughMessageSequence: 1,
        toolMembers: [
          {
            memberKind: "public_tool_use",
            modelToolCallId: "call_1",
            toolUseEventId: "event_tool_1",
            toolName: "Read",
          },
          {
            memberKind: "internal_tool_repair",
            modelToolCallId: "call_1",
            toolName: "missing_tool",
            repairEventId: "event_repair_1",
            outcome: "error",
          },
        ],
      },
    } as const;

    expect(() => ThreadTurnCheckpointSchema.parse(duplicateCallId)).toThrow(
      "modelToolCallId must be unique within a request",
    );
    expect(() => ThreadTurnCheckpointSchema.parse({
      pendingInputMessageIds: ["msg_1", "msg_1"],
    })).toThrow("pendingInputMessageIds must be unique");
    expect(() => ThreadTurnCheckpointSchema.parse({
      pendingInputMessageIds: [],
      request: {
        modelRequestId: "request_1",
        requestStartEventId: "event_start",
        requestKind: "agent_provider_request",
        contextThroughMessageSequence: -1,
        toolMembers: [],
      },
    })).toThrow();
    expect(() => ThreadTurnCheckpointSchema.parse({
      pendingInputMessageIds: [],
      request: {
        modelRequestId: "request_1",
        requestStartEventId: "event_start",
        requestKind: "agent_provider_request",
        contextThroughMessageSequence: Number.MAX_SAFE_INTEGER + 1,
        toolMembers: [],
      },
    })).toThrow();
  });

  test("rejects duplicate or incomplete route ownership", () => {
    expect(() => ThreadToolRouteViewSchema.parse({
      routes: [
        { toolUseEventId: "event_tool_1", disposition: "hot_execution" },
        { toolUseEventId: "event_tool_1", disposition: "requires_user_action" },
      ],
    })).toThrow("tool route ownership must be unique");
    expect(() => ThreadToolRouteViewSchema.parse({
      routes: [{ toolUseEventId: "", disposition: "hot_execution" }],
    })).toThrow();
  });

  test("rejects a requires-action closeout after its sealed request was erased", () => {
    expect(() => ThreadTurnCheckpointSchema.parse({
      pendingInputMessageIds: [],
      idleCloseout: { eventId: "event_idle", stopReason: "requires_action" },
    })).toThrow("requires_action closeout must retain a sealed incomplete Tool Use");
    expect(() => ThreadTurnCheckpointSchema.parse({
      pendingInputMessageIds: [],
      request: {
        modelRequestId: "request_1",
        requestStartEventId: "event_start",
        requestKind: "agent_provider_request",
        contextThroughMessageSequence: 1,
        requestEnd: { eventId: "event_end", isError: false, rescheduled: false },
        toolMembers: [],
      },
      idleCloseout: { eventId: "event_idle", stopReason: "requires_action" },
    })).toThrow("requires_action closeout must retain a sealed incomplete Tool Use");
  });
});
