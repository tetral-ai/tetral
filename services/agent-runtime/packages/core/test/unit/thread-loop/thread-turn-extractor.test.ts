import { describe, expect, test } from "bun:test";
import { DurableRuntimeMessageSchema } from "../../../src/contracts/runtime.js";
import {
  extractColdThreadToolRouteView,
  extractThreadTurnCheckpoint,
  ThreadTurnLoadFactsSchema,
} from "../../../src/thread-loop/thread-turn-extractor.js";

const createdAt = "2026-08-03T00:00:00.000Z";

function durableTextMessage(input: {
  readonly id: string;
  readonly sequence: number;
  readonly eventId: string;
  readonly eventSequence: number;
  readonly role: "user" | "assistant";
}) {
  return DurableRuntimeMessageSchema.parse({
    id: input.id,
    sessionId: "session_1",
    role: input.role,
    origin: input.role === "user" ? "user" : "agent",
    sequence: input.sequence,
    status: "completed",
    owningEventId: input.eventId,
    eventSequence: input.eventSequence,
    createdAt,
    parts: [{
      id: `${input.id}_part`,
      sessionId: "session_1",
      messageId: input.id,
      sequence: 0,
      type: "text",
      text: input.id,
      truncated: false,
      status: "completed",
      createdAt,
    }],
  });
}

function durableToolMessage() {
  return DurableRuntimeMessageSchema.parse({
    id: "message_assistant",
    sessionId: "session_1",
    role: "assistant",
    origin: "agent",
    sequence: 2,
    status: "completed",
    owningEventId: "event_tool_result",
    eventSequence: 6,
    createdAt,
    parts: [{
      id: "message_assistant_tool",
      sessionId: "session_1",
      messageId: "message_assistant",
      sequence: 0,
      type: "tool",
      toolCallId: "tool_call_1",
      toolName: "Read",
      toolUseEventId: "event_tool_use",
      toolEvent: { kind: "tool" },
      state: {
        status: "completed",
        input: { value: {}, preview: "{}", truncated: false },
        output: { text: "done", truncated: false },
      },
      createdAt,
      completedAt: createdAt,
    }],
  });
}

function durableOpenToolMessage() {
  return DurableRuntimeMessageSchema.parse({
    id: "message_tool_use",
    sessionId: "session_1",
    role: "assistant",
    origin: "agent",
    sequence: 2,
    status: "streaming",
    owningEventId: "event_tool_use",
    eventSequence: 4,
    createdAt,
    parts: [{
      id: "message_tool_use_part",
      sessionId: "session_1",
      messageId: "message_tool_use",
      sequence: 0,
      type: "tool",
      toolCallId: "tool_call_1",
      toolName: "Read",
      toolUseEventId: "event_tool_use",
      toolEvent: { kind: "tool" },
      state: {
        status: "running",
        input: { value: {}, preview: "{}", truncated: false },
      },
      createdAt,
    }],
  });
}

function durableInternalRepairMessage() {
  return DurableRuntimeMessageSchema.parse({
    id: "message_repair",
    sessionId: "session_1",
    role: "assistant",
    origin: "agent",
    sequence: 1,
    status: "completed",
    owningEventId: "event_repair",
    eventSequence: 2,
    createdAt,
    parts: [{
      id: "message_repair_tool",
      sessionId: "session_1",
      messageId: "message_repair",
      sequence: 0,
      type: "tool",
      toolCallId: "tool_call_repair",
      toolName: "unknown_tool",
      toolEvent: { kind: "tool" },
      state: {
        status: "error",
        input: { value: {}, preview: "{}", truncated: false },
        error: { type: "invalid_tool", message: "invalid tool" },
      },
      createdAt,
      completedAt: createdAt,
    }],
  });
}

function durableIdleCleanupMessage() {
  return DurableRuntimeMessageSchema.parse({
    id: "message_idle_cleanup",
    sessionId: "session_1",
    role: "assistant",
    origin: "agent",
    sequence: 3,
    status: "completed",
    owningEventId: "event_tool_result",
    eventSequence: 6,
    createdAt,
    parts: [{
      id: "message_idle_cleanup_tool",
      sessionId: "session_1",
      messageId: "message_idle_cleanup",
      sequence: 0,
      type: "tool",
      toolCallId: "tool_call_1",
      toolName: "Read",
      toolUseEventId: "event_tool_use",
      toolEvent: { kind: "tool" },
      state: {
        status: "error",
        input: { value: {}, preview: "{}", truncated: false },
        error: { type: "cleanup_expired", message: "expired" },
      },
      createdAt,
      completedAt: createdAt,
    }],
  });
}

describe("Thread turn cold extraction", () => {
  test("reconstructs sealed membership and only later durable input", () => {
    const messages = [
      durableTextMessage({
        id: "message_consumed",
        sequence: 1,
        eventId: "event_input_1",
        eventSequence: 1,
        role: "user",
      }),
      durableToolMessage(),
      durableTextMessage({
        id: "message_pending",
        sequence: 3,
        eventId: "event_input_2",
        eventSequence: 7,
        role: "user",
      }),
    ];
    const facts = ThreadTurnLoadFactsSchema.parse({
      events: [
        {
          eventId: "event_running",
          eventSequence: 2,
          type: "session.status_running",
        },
        {
          eventId: "event_start",
          eventSequence: 3,
          type: "span.model_request_start",
          modelRequestId: "model_request_1",
          requestStart: {
            requestKind: "agent_provider_request",
            contextThroughMessageSequence: 1,
          },
        },
        {
          eventId: "event_tool_use",
          eventSequence: 4,
          type: "agent.tool_use",
          modelRequestId: "model_request_1",
          toolUse: {
            modelToolCallId: "tool_call_1",
            toolName: "Read",
          },
        },
        {
          eventId: "event_end",
          eventSequence: 5,
          type: "span.model_request_end",
          modelRequestId: "model_request_1",
          requestEnd: {
            requestStartEventId: "event_start",
            isError: false,
            rescheduled: false,
          },
        },
        {
          eventId: "event_tool_result",
          eventSequence: 6,
          type: "agent.tool_result",
          modelRequestId: "model_request_1",
          toolResult: {
            toolUseEventId: "event_tool_use",
            outcome: "success",
          },
        },
      ],
      messageLineage: [
        {
          messageId: "message_consumed",
          messageSequence: 1,
          owningEventId: "event_input_1",
          entries: [{
            lineageKind: "declaration_receipt",
            operationKind: "commit_inputs",
            sourceKind: "messages",
            sourceId: "input_1",
            eventId: "event_input_1",
            eventSequence: 1,
            disposition: "created",
          }],
        },
        {
          messageId: "message_assistant",
          messageSequence: 2,
          owningEventId: "event_tool_result",
          modelRequestId: "model_request_1",
          entries: [
            {
              lineageKind: "declaration_receipt",
              operationKind: "write_event",
              sourceKind: "agent.tool_use",
              sourceId: "write_tool_use",
              eventId: "event_tool_use",
              eventSequence: 4,
              disposition: "created",
            },
            {
              lineageKind: "declaration_receipt",
              operationKind: "write_event",
              sourceKind: "agent.tool_result",
              sourceId: "write_result",
              eventId: "event_tool_result",
              eventSequence: 6,
              disposition: "updated",
            },
          ],
        },
        {
          messageId: "message_pending",
          messageSequence: 3,
          owningEventId: "event_input_2",
          entries: [{
            lineageKind: "declaration_receipt",
            operationKind: "commit_inputs",
            sourceKind: "messages",
            sourceId: "input_2",
            eventId: "event_input_2",
            eventSequence: 7,
            disposition: "created",
          }],
        },
      ],
    });

    expect(extractThreadTurnCheckpoint({ messages, facts })).toEqual({
      executionRunId: "event_running",
      pendingInputMessageIds: ["message_pending"],
      request: {
        modelRequestId: "model_request_1",
        requestStartEventId: "event_start",
        requestKind: "agent_provider_request",
        contextThroughMessageSequence: 1,
        requestEnd: {
          eventId: "event_end",
          isError: false,
          rescheduled: false,
        },
        toolMembers: [{
          memberKind: "public_tool_use",
          modelToolCallId: "tool_call_1",
          toolUseEventId: "event_tool_use",
          toolName: "Read",
          terminalResult: {
            resultEventId: "event_tool_result",
            outcome: "success",
          },
        }],
      },
    });
  });

  test("rejects post-seal members and unknown fact shapes", () => {
    expect(() => ThreadTurnLoadFactsSchema.parse({
      events: [{ eventId: "event_unknown", eventSequence: 1, type: "unknown.event" }],
      messageLineage: [],
    })).toThrow();

    const facts = ThreadTurnLoadFactsSchema.parse({
      events: [
        {
          eventId: "event_start",
          eventSequence: 1,
          type: "span.model_request_start",
          modelRequestId: "model_request_1",
          requestStart: { requestKind: "agent_provider_request", contextThroughMessageSequence: 0 },
        },
        {
          eventId: "event_end",
          eventSequence: 2,
          type: "span.model_request_end",
          modelRequestId: "model_request_1",
          requestEnd: { requestStartEventId: "event_start", isError: false, rescheduled: false },
        },
        {
          eventId: "event_tool_use",
          eventSequence: 3,
          type: "agent.tool_use",
          modelRequestId: "model_request_1",
          toolUse: { modelToolCallId: "tool_call_1", toolName: "Read" },
        },
      ],
      messageLineage: [],
    });
    expect(() => extractThreadTurnCheckpoint({ messages: [], facts })).toThrow(
      "Tool Use cannot follow Request End",
    );
  });

  test("joins each cold non-terminal Tool Use to exactly one durable route", () => {
    const checkpoint = {
      pendingInputMessageIds: [],
      request: {
        modelRequestId: "model_request_1",
        requestStartEventId: "event_start",
        requestKind: "agent_provider_request" as const,
        contextThroughMessageSequence: 0,
        requestEnd: {
          eventId: "event_end",
          isError: false,
          rescheduled: false,
        },
        toolMembers: [
          {
            memberKind: "public_tool_use" as const,
            modelToolCallId: "tool_call_approval",
            toolUseEventId: "event_tool_approval",
            toolName: "Write",
          },
          {
            memberKind: "public_tool_use" as const,
            modelToolCallId: "tool_call_sandbox",
            toolUseEventId: "event_tool_sandbox",
            toolName: "Bash",
          },
        ],
      },
    };
    const pendingToolUses = [{
      toolUseEventId: "event_tool_approval",
      modelRequestId: "model_request_1",
      modelToolCallId: "tool_call_approval",
      toolName: "Write",
      status: "pending" as const,
    }];
    const pendingSandboxExecutions = [{
      toolUseEventId: "event_tool_sandbox",
      modelRequestId: "model_request_1",
      modelToolCallId: "tool_call_sandbox",
      toolName: "Bash",
    }];

    expect(extractColdThreadToolRouteView({
      checkpoint,
      pendingToolUses,
      pendingSandboxExecutions,
    })).toEqual({
      routes: [
        { toolUseEventId: "event_tool_approval", disposition: "requires_user_action" },
        { toolUseEventId: "event_tool_sandbox", disposition: "resume_sandbox_execution" },
      ],
    });

    expect(() => extractColdThreadToolRouteView({
      checkpoint,
      pendingToolUses: [],
      pendingSandboxExecutions,
    })).toThrow("cold non-terminal Tool Use requires exactly one durable route");
    expect(() => extractColdThreadToolRouteView({
      checkpoint,
      pendingToolUses,
      pendingSandboxExecutions: [
        pendingSandboxExecutions[0]!,
        { ...pendingSandboxExecutions[0]!, toolUseEventId: "event_tool_approval" },
      ],
    })).toThrow("cold Tool route identity does not match its request member");
  });

  test("rejects an unknown declaration lineage combination", () => {
    const message = durableTextMessage({
      id: "message_unknown",
      sequence: 1,
      eventId: "event_unknown",
      eventSequence: 1,
      role: "assistant",
    });
    const facts = ThreadTurnLoadFactsSchema.parse({
      events: [],
      messageLineage: [{
        messageId: message.id,
        messageSequence: message.sequence,
        owningEventId: message.owningEventId,
        entries: [{
          lineageKind: "declaration_receipt",
          operationKind: "invented_operation",
          sourceKind: "invented_source",
          sourceId: "invented_source_1",
          eventId: message.owningEventId,
          eventSequence: message.eventSequence,
          disposition: "created",
        }],
      }],
    });

    expect(() => extractThreadTurnCheckpoint({ messages: [message], facts })).toThrow(
      "message declaration lineage is not recognized",
    );

    const unknownWriteEvent = ThreadTurnLoadFactsSchema.parse({
      events: [],
      messageLineage: [{
        messageId: message.id,
        messageSequence: message.sequence,
        owningEventId: message.owningEventId,
        entries: [{
          lineageKind: "declaration_receipt",
          operationKind: "write_event",
          sourceKind: "invented_future_event",
          sourceId: "write_unknown_1",
          eventId: message.owningEventId,
          eventSequence: message.eventSequence,
          disposition: "created",
        }],
      }],
    });
    expect(() => extractThreadTurnCheckpoint({ messages: [message], facts: unknownWriteEvent })).toThrow(
      "message declaration lineage is not recognized",
    );
  });

  test("accepts every streaming Tool Use lineage entry on one assistant message", () => {
    const message = DurableRuntimeMessageSchema.parse({
      ...durableToolMessage(),
      owningEventId: "event_tool_use_2",
      eventSequence: 3,
      status: "streaming",
      parts: [
        {
          ...durableToolMessage().parts[0],
          state: { status: "pending" },
        },
        {
          ...durableToolMessage().parts[0],
          id: "message_assistant_tool_2",
          sequence: 1,
          toolCallId: "tool_call_2",
          toolName: "Bash",
          toolUseEventId: "event_tool_use_2",
          state: { status: "pending" },
        },
      ],
    });
    const facts = ThreadTurnLoadFactsSchema.parse({
      events: [
        {
          eventId: "event_start",
          eventSequence: 1,
          type: "span.model_request_start",
          modelRequestId: "model_request_1",
          requestStart: { requestKind: "agent_provider_request", contextThroughMessageSequence: 0 },
        },
        {
          eventId: "event_tool_use",
          eventSequence: 2,
          type: "agent.tool_use",
          modelRequestId: "model_request_1",
          toolUse: { modelToolCallId: "tool_call_1", toolName: "Read" },
        },
        {
          eventId: "event_tool_use_2",
          eventSequence: 3,
          type: "agent.tool_use",
          modelRequestId: "model_request_1",
          toolUse: { modelToolCallId: "tool_call_2", toolName: "Bash" },
        },
      ],
      messageLineage: [{
        messageId: message.id,
        messageSequence: message.sequence,
        owningEventId: message.owningEventId,
        modelRequestId: "model_request_1",
        entries: [
          {
            lineageKind: "declaration_receipt",
            operationKind: "write_event",
            sourceKind: "agent.tool_use",
            sourceId: "write_tool_use_1",
            eventId: "event_tool_use",
            eventSequence: 2,
            disposition: "created",
          },
          {
            lineageKind: "declaration_receipt",
            operationKind: "write_event",
            sourceKind: "agent.tool_use",
            sourceId: "write_tool_use_2",
            eventId: "event_tool_use_2",
            eventSequence: 3,
            disposition: "updated",
          },
        ],
      }],
    });

    expect(extractThreadTurnCheckpoint({ messages: [message], facts }).request?.toolMembers).toHaveLength(2);
  });

  test("rejects a Request Start boundary above the loaded durable message range", () => {
    const message = durableTextMessage({
      id: "message_input",
      sequence: 1,
      eventId: "event_input",
      eventSequence: 1,
      role: "user",
    });
    const facts = ThreadTurnLoadFactsSchema.parse({
      events: [{
        eventId: "event_start",
        eventSequence: 2,
        type: "span.model_request_start",
        modelRequestId: "model_request_1",
        requestStart: { requestKind: "agent_provider_request", contextThroughMessageSequence: 99 },
      }],
      messageLineage: [{
        messageId: message.id,
        messageSequence: message.sequence,
        owningEventId: message.owningEventId,
        entries: [{
          lineageKind: "declaration_receipt",
          operationKind: "commit_inputs",
          sourceKind: "messages",
          sourceId: "input_1",
          eventId: message.owningEventId,
          eventSequence: message.eventSequence,
          disposition: "created",
        }],
      }],
    });

    expect(() => extractThreadTurnCheckpoint({ messages: [message], facts })).toThrow(
      "Request Start message boundary exceeds the loaded durable message range",
    );
  });

  test("requires Tool Use and internal repair facts to have matching declaration lineage", () => {
    const publicMessage = durableToolMessage();
    const publicFacts = ThreadTurnLoadFactsSchema.parse({
      events: [
        {
          eventId: "event_start",
          eventSequence: 1,
          type: "span.model_request_start",
          modelRequestId: "model_request_1",
          requestStart: { requestKind: "agent_provider_request", contextThroughMessageSequence: 0 },
        },
        {
          eventId: "event_tool_use",
          eventSequence: 4,
          type: "agent.tool_use",
          modelRequestId: "model_request_1",
          toolUse: { modelToolCallId: "tool_call_1", toolName: "Read" },
        },
      ],
      messageLineage: [{
        messageId: publicMessage.id,
        messageSequence: publicMessage.sequence,
        owningEventId: publicMessage.owningEventId,
        modelRequestId: "model_request_1",
        entries: [{
          lineageKind: "declaration_receipt",
          operationKind: "commit_inputs",
          sourceKind: "messages",
          sourceId: "input_wrong_owner",
          eventId: "event_tool_use",
          eventSequence: 4,
          disposition: "created",
        }],
      }],
    });
    expect(() => extractThreadTurnCheckpoint({ messages: [publicMessage], facts: publicFacts })).toThrow(
      "message declaration lineage does not match its message projection",
    );

    const repairMessage = durableInternalRepairMessage();
    const repairFacts = ThreadTurnLoadFactsSchema.parse({
      events: [
        {
          eventId: "event_start",
          eventSequence: 1,
          type: "span.model_request_start",
          modelRequestId: "model_request_1",
          requestStart: { requestKind: "agent_provider_request", contextThroughMessageSequence: 0 },
        },
        {
          eventId: "event_repair",
          eventSequence: 2,
          type: "agent.tool_result",
          modelRequestId: "model_request_1",
          toolResult: {
            modelToolCallId: "tool_call_repair",
            toolName: "unknown_tool",
            repairKind: "invalid_tool",
            outcome: "error",
          },
        },
      ],
      messageLineage: [{
        messageId: repairMessage.id,
        messageSequence: repairMessage.sequence,
        owningEventId: repairMessage.owningEventId,
        modelRequestId: "model_request_1",
        entries: [{
          lineageKind: "declaration_receipt",
          operationKind: "write_event",
          sourceKind: "agent.tool_result",
          sourceId: "wrong_repair_writer",
          eventId: "event_repair",
          eventSequence: 2,
          disposition: "created",
        }],
      }],
    });
    expect(() => extractThreadTurnCheckpoint({ messages: [repairMessage], facts: repairFacts })).toThrow(
      "internal repair has no matching declaration lineage",
    );
  });

  test("reconstructs a receipt-backed internal repair as one terminal member", () => {
    const message = durableInternalRepairMessage();
    const facts = ThreadTurnLoadFactsSchema.parse({
      events: [
        {
          eventId: "event_start",
          eventSequence: 1,
          type: "span.model_request_start",
          modelRequestId: "model_request_1",
          requestStart: { requestKind: "agent_provider_request", contextThroughMessageSequence: 0 },
        },
        {
          eventId: "event_repair",
          eventSequence: 2,
          type: "agent.tool_result",
          modelRequestId: "model_request_1",
          toolResult: {
            modelToolCallId: "tool_call_repair",
            toolName: "unknown_tool",
            repairKind: "invalid_tool",
            outcome: "error",
          },
        },
        {
          eventId: "event_end",
          eventSequence: 3,
          type: "span.model_request_end",
          modelRequestId: "model_request_1",
          requestEnd: { requestStartEventId: "event_start", isError: false, rescheduled: false },
        },
      ],
      messageLineage: [{
        messageId: message.id,
        messageSequence: message.sequence,
        owningEventId: message.owningEventId,
        modelRequestId: "model_request_1",
        entries: [{
          lineageKind: "declaration_receipt",
          operationKind: "commit_internal_tool_repair",
          sourceKind: "internal_tool_repair",
          sourceId: "repair_1",
          eventId: "event_repair",
          eventSequence: 2,
          disposition: "created",
        }],
      }],
    });

    expect(extractThreadTurnCheckpoint({ messages: [message], facts }).request?.toolMembers).toEqual([{
      memberKind: "internal_tool_repair",
      modelToolCallId: "tool_call_repair",
      toolName: "unknown_tool",
      repairEventId: "event_repair",
      outcome: "error",
    }]);
  });

  test("reconstructs bridge_pod_loss_repair as a terminal result without a declaration receipt", () => {
    const message = durableToolMessage();
    const facts = ThreadTurnLoadFactsSchema.parse({
      events: [
        {
          eventId: "event_start",
          eventSequence: 3,
          type: "span.model_request_start",
          modelRequestId: "model_request_1",
          requestStart: { requestKind: "agent_provider_request", contextThroughMessageSequence: 0 },
        },
        {
          eventId: "event_tool_use",
          eventSequence: 4,
          type: "agent.tool_use",
          modelRequestId: "model_request_1",
          toolUse: { modelToolCallId: "tool_call_1", toolName: "Read" },
        },
        {
          eventId: "event_end",
          eventSequence: 5,
          type: "span.model_request_end",
          modelRequestId: "model_request_1",
          requestEnd: { requestStartEventId: "event_start", isError: false, rescheduled: false },
        },
        {
          eventId: "event_tool_result",
          eventSequence: 6,
          type: "agent.tool_result",
          modelRequestId: "model_request_1",
          toolResult: { toolUseEventId: "event_tool_use", outcome: "error" },
        },
      ],
      messageLineage: [{
        messageId: message.id,
        messageSequence: message.sequence,
        owningEventId: message.owningEventId,
        modelRequestId: "model_request_1",
        entries: [
          {
            lineageKind: "declaration_receipt",
            operationKind: "write_event",
            sourceKind: "agent.tool_use",
            sourceId: "write_tool_use",
            eventId: "event_tool_use",
            eventSequence: 4,
            disposition: "created",
          },
          {
              lineageKind: "bridge_pod_loss_repair",
            repairEventId: "event_tool_result",
            eventSequence: 6,
            toolUseEventId: "event_tool_use",
            outcome: "error",
          },
        ],
      }],
    });

    expect(extractThreadTurnCheckpoint({ messages: [message], facts }).request?.toolMembers).toEqual([{
      memberKind: "public_tool_use",
      modelToolCallId: "tool_call_1",
      toolUseEventId: "event_tool_use",
      toolName: "Read",
      terminalResult: { resultEventId: "event_tool_result", outcome: "error" },
    }]);
  });

  test("reconstructs an idle-cleanup result message created without a declaration receipt", () => {
    const toolMessage = durableOpenToolMessage();
    const resultMessage = durableIdleCleanupMessage();
    const facts = {
      events: [
        {
          eventId: "event_start",
          eventSequence: 3,
          type: "span.model_request_start" as const,
          modelRequestId: "model_request_1",
          requestStart: { requestKind: "agent_provider_request" as const, contextThroughMessageSequence: 0 },
        },
        {
          eventId: "event_tool_use",
          eventSequence: 4,
          type: "agent.tool_use" as const,
          modelRequestId: "model_request_1",
          toolUse: { modelToolCallId: "tool_call_1", toolName: "Read" },
        },
        {
          eventId: "event_end",
          eventSequence: 5,
          type: "span.model_request_end" as const,
          modelRequestId: "model_request_1",
          requestEnd: { requestStartEventId: "event_start", isError: false, rescheduled: false },
        },
        {
          eventId: "event_tool_result",
          eventSequence: 6,
          type: "agent.tool_result" as const,
          modelRequestId: "model_request_1",
          toolResult: { toolUseEventId: "event_tool_use", outcome: "error" as const },
        },
      ],
      messageLineage: [
        {
          messageId: toolMessage.id,
          messageSequence: toolMessage.sequence,
          owningEventId: "event_tool_use",
          modelRequestId: "model_request_1",
          entries: [{
            lineageKind: "declaration_receipt" as const,
            operationKind: "write_event",
            sourceKind: "agent.tool_use",
            sourceId: "write_tool_use",
            eventId: "event_tool_use",
            eventSequence: 4,
            disposition: "created" as const,
          }],
        },
        {
          messageId: resultMessage.id,
          messageSequence: resultMessage.sequence,
          owningEventId: resultMessage.owningEventId,
          modelRequestId: "model_request_1",
          entries: [{
            lineageKind: "bridge_idle_cleanup_repair" as const,
            repairEventId: "event_tool_result",
            eventSequence: 6,
            toolUseEventId: "event_tool_use",
            outcome: "error" as const,
          }],
        },
      ],
    };

    expect(extractThreadTurnCheckpoint({ messages: [toolMessage, resultMessage], facts }).request?.toolMembers).toEqual([{
      memberKind: "public_tool_use",
      modelToolCallId: "tool_call_1",
      toolUseEventId: "event_tool_use",
      toolName: "Read",
      terminalResult: { resultEventId: "event_tool_result", outcome: "error" },
    }]);
  });

  test("rejects an assistant message disguised as pending committed input", () => {
    const message = durableTextMessage({
      id: "message_assistant_input",
      sequence: 1,
      eventId: "event_assistant",
      eventSequence: 1,
      role: "assistant",
    });
    expect(() => extractThreadTurnCheckpoint({
      messages: [message],
      facts: {
        events: [],
        messageLineage: [{
          messageId: message.id,
          messageSequence: message.sequence,
          owningEventId: message.owningEventId,
          entries: [{
            lineageKind: "declaration_receipt",
            operationKind: "commit_inputs",
            sourceKind: "messages",
            sourceId: "input_1",
            eventId: message.owningEventId,
            eventSequence: message.eventSequence,
            disposition: "created",
          }],
        }],
      },
    })).toThrow("message declaration lineage does not match its message projection");
  });

  test("does not resurrect an interrupt after its execution run is durably idle", () => {
    const facts = ThreadTurnLoadFactsSchema.parse({
      events: [
        { eventId: "event_running", eventSequence: 1, type: "session.status_running" },
        { eventId: "event_interrupt", eventSequence: 2, type: "user.interrupt" },
        {
          eventId: "event_idle",
          eventSequence: 3,
          type: "session.status_idle",
          idle: { stopReason: "end_turn" },
        },
      ],
      messageLineage: [],
    });

    expect(extractThreadTurnCheckpoint({ messages: [], facts })).toEqual({
      pendingInputMessageIds: [],
      idleCloseout: { eventId: "event_idle", stopReason: "end_turn" },
    });
  });
});
