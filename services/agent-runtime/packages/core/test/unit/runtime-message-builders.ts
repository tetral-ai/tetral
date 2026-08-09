// Shared RuntimeMessage fixtures preserve suite-specific wire values while keeping
// schema construction in one auditable test module.

import type {
  DurableRuntimeMessage,
  RuntimeJsonValue,
  RuntimeMessage,
  RuntimeMessageCreate,
  RuntimePart,
} from "../../src/contracts/runtime.js";
import { DurableRuntimeMessageSchema, RuntimeMessageSchema } from "../../src/contracts/runtime.js";
import type {
  RuntimeControlInputCommitResult,
  RuntimeControlInputDeclaration,
  RuntimeThreadControlState,
} from "../../src/thread-loop/thread-state.js";

export function buildRuntimeControlCommitResult(
  scope: RuntimeThreadControlState,
  inputKind: "interrupt_control" | "tool_confirmation",
  declaration: RuntimeControlInputDeclaration,
): RuntimeControlInputCommitResult {
  const createdAt = "2026-01-01T00:00:00.000Z";
  return {
    ok: true,
    receipt: {
      sessionThreadId: scope.sessionThreadId,
      operationKind: "commit_inputs",
      sourceKind: inputKind,
      operationId: scope.runtimeInputId,
      declarationDigest: "digest_test",
      events: scope.eventIds.map((eventId, index) => ({
        sessionThreadId: scope.sessionThreadId,
        eventId,
        eventSequence: scope.sequenceFrom + index,
        disposition: "existing" as const,
      })),
		messages: declaration.messageCreates.map((create, index) =>
		runtimeControlMessageStamp(scope, create, index, createdAt)
      ),
      pendingAttachmentDelta: [],
		interruptToolProjections: [],
      prefixConsumptions: [],
      childLifecycle: [],
    },
  };
}

function runtimeControlMessageStamp(
  scope: RuntimeThreadControlState,
  create: RuntimeMessageCreate,
  index: number,
  createdAt: string,
) {
  const messageId = `msg_control_${scope.runtimeInputId}_${index}`;
  return {
    sessionThreadId: scope.sessionThreadId,
		owningEventId: create.sourceEventId ?? scope.eventIds[index]!,
    messageId,
    messageSequence: scope.sequenceTo + index + 1,
    createdAt,
    updatedAt: "",
    disposition: "created" as const,
		parts: create.parts.map((_part, partIndex) => ({
      partId: `part_control_${scope.runtimeInputId}_${index}_${partIndex}`,
      messageId,
      partSequence: partIndex,
      createdAt,
      updatedAt: "",
      disposition: "created" as const,
    })),
  };
}

export function buildThreadLoopUserMessage(id: string, sequence: number, text: string): RuntimeMessage {
  const createdAt = "2026-06-14T00:00:00.000Z";
  return RuntimeMessageSchema.parse({
    id,
    sessionId: "sesn_1",
    role: "user",
    origin: "user",
    sequence,
    status: "completed",
    createdAt,
    parts: [
      {
        id: `${id}-text`,
        sessionId: "sesn_1",
        messageId: id,
        sequence: 0,
        type: "text",
        text,
        truncated: false,
        status: "completed",
        createdAt,
      },
    ],
  });
}

export function buildThreadLoopRuntimeNotificationMessage(id: string, text: string): RuntimeMessage {
  const createdAt = "2026-06-14T00:00:00.000Z";
  return RuntimeMessageSchema.parse({
    id,
    sessionId: "sesn_1",
    role: "user",
    origin: "runtime",
    sequence: 0,
    status: "completed",
    createdAt,
    updatedAt: createdAt,
    parts: [
      {
        id: `${id}-text`,
        sessionId: "sesn_1",
        messageId: id,
        sequence: 0,
        type: "text",
        text,
        truncated: false,
        status: "completed",
        createdAt,
        updatedAt: createdAt,
        completedAt: createdAt,
      },
    ],
  });
}

export function buildThreadLoopDurableRuntimeNotificationMessage(id: string, text: string): DurableRuntimeMessage {
  const message = buildThreadLoopRuntimeNotificationMessage(id, text);
  return DurableRuntimeMessageSchema.parse({
    ...message,
    owningEventId: `${id}-event`,
    eventSequence: 1,
  });
}

export function buildContextLoaderTextMessage(id: string, sessionId: string, role: "user" | "assistant", sequence: number, text: string): RuntimeMessage {
  const createdAt = "2026-06-14T00:00:00.000Z";
  return RuntimeMessageSchema.parse({
    id,
    sessionId,
    role,
    origin: role === "user" ? "user" : "agent",
    sequence,
    status: "completed",
    createdAt,
    parts: [
      {
        id: `${id}-text`,
        sessionId,
        messageId: id,
        sequence: 0,
        type: "text",
        text,
        truncated: false,
        status: "completed",
        createdAt,
      },
    ],
  });
}

export function buildContextLoaderAssistantToolMessage(toolStatus: "pending" | "running"): RuntimeMessage {
  const createdAt = "2026-06-14T00:00:00.000Z";
  const id = `assistant-tool-${toolStatus}`;
  return RuntimeMessageSchema.parse({
    id,
    sessionId: "sesn_1",
    role: "assistant",
    origin: "agent",
    sequence: 3,
    status: "completed",
    createdAt,
    parts: [
      {
        id: `${id}-part`,
        sessionId: "sesn_1",
        messageId: id,
        sequence: 0,
        type: "tool",
        toolCallId: `${id}-call`,
        toolName: "lookup",
        state:
          toolStatus === "pending"
            ? { status: "pending" }
            : { status: "running", input: { value: { q: "x" }, preview: "{\"q\":\"x\"}", truncated: false } },
        createdAt,
      },
    ],
  });
}

export function buildContextLoaderStructuralAssistantMessage(): RuntimeMessage {
  const createdAt = "2026-06-14T00:00:00.000Z";
  return RuntimeMessageSchema.parse({
    id: "assistant-structural",
    sessionId: "sesn_1",
    role: "assistant",
    origin: "agent",
    sequence: 4,
    status: "completed",
    createdAt,
    parts: [
      {
        id: "assistant-structural-step",
        sessionId: "sesn_1",
        messageId: "assistant-structural",
        sequence: 0,
        type: "step-start",
        createdAt,
      },
    ],
  });
}

export function buildContextLoaderAbortedVisibleAssistantMessage(): RuntimeMessage {
  return RuntimeMessageSchema.parse({
    ...buildContextLoaderTextMessage("assistant-aborted", "sesn_1", "assistant", 5, "partial answer"),
    status: "cancelled",
  });
}

export function buildContextLoaderFailedVisibleAssistantMessage(): RuntimeMessage {
  return RuntimeMessageSchema.parse({
    ...buildContextLoaderTextMessage("assistant-failed", "sesn_1", "assistant", 6, "internal failure text"),
    status: "failed",
    error: {
      type: "runtime",
      code: "runtime_invalid_sequence",
      message: "assistant failed",
      retryable: false,
      fatal: true,
      retryStatus: { type: "terminal" },
      reason: "runtime_contract_validation",
    },
  });
}

export function buildContextLoaderAssistantMessageWithUsage(): RuntimeMessage {
  return RuntimeMessageSchema.parse({
    ...buildContextLoaderTextMessage("assistant-usage", "sesn_1", "assistant", 7, "answer"),
    usage: {
      inputTokens: 11,
      outputTokens: 7,
      reasoningTokens: 0,
      cacheReadTokens: 3,
      cacheWriteTokens: 2,
      totalTokens: 18,
    },
  });
}

export function buildContextLoaderStreamingAssistantMessage(): RuntimeMessage {
  const createdAt = "2026-06-14T00:00:00.000Z";
  return RuntimeMessageSchema.parse({
    id: "assistant-streaming",
    sessionId: "sesn_1",
    role: "assistant",
    origin: "agent",
    sequence: 8,
    status: "streaming",
    createdAt,
    parts: [
      {
        id: "assistant-streaming-text",
        sessionId: "sesn_1",
        messageId: "assistant-streaming",
        sequence: 0,
        type: "text",
        text: "partial answer",
        truncated: false,
        status: "streaming",
        startedAt: createdAt,
        createdAt,
      },
      {
        id: "assistant-streaming-reasoning",
        sessionId: "sesn_1",
        messageId: "assistant-streaming",
        sequence: 1,
        type: "reasoning",
        text: "partial reasoning",
        truncated: false,
        status: "streaming",
        startedAt: createdAt,
        createdAt,
      },
    ],
  });
}

export function buildConversationTurnsTextMessage(
  id: string,
  role: "user" | "assistant",
  origin: "user" | "agent" | "runtime",
  sequence: number,
  text: string,
): RuntimeMessage {
  const now = "2026-07-10T00:00:00.000Z";
  return RuntimeMessageSchema.parse({
    id,
    sessionId: "session-1",
    role,
    origin,
    sequence,
    status: "completed",
    createdAt: now,
    parts: [{
      id: `${id}-part`,
      sessionId: "session-1",
      messageId: id,
      sequence: 0,
      type: "text",
      text,
      truncated: false,
      status: "completed",
      createdAt: now,
      completedAt: now,
    }],
  });
}

export function buildConversationTurnsToolMessage(id: string, sequence: number, toolCallId: string, toolName: string): RuntimeMessage {
  const now = "2026-07-10T00:00:00.000Z";
  return RuntimeMessageSchema.parse({
    id,
    sessionId: "session-1",
    role: "assistant",
    origin: "agent",
    sequence,
    status: "completed",
    createdAt: now,
    parts: [conversationTurnsToolPart(id, toolCallId, toolName, `event-${toolCallId}`)],
  });
}

export function buildConversationTurnsInternalRepairMessage(id: string, sequence: number): RuntimeMessage {
  const now = "2026-07-10T00:00:00.000Z";
  return RuntimeMessageSchema.parse({
    id,
    sessionId: "session-1",
    role: "assistant",
    origin: "agent",
    sequence,
    status: "completed",
    createdAt: now,
    parts: [conversationTurnsToolPart(id, "repair-call", "repair-tool", undefined)],
  });
}

function conversationTurnsToolPart(messageId: string, toolCallId: string, toolName: string, toolUseEventId: string | undefined) {
  const now = "2026-07-10T00:00:00.000Z";
  return {
    id: `${messageId}-part`,
    sessionId: "session-1",
    messageId,
    sequence: 0,
    type: "tool" as const,
    toolCallId,
    toolName,
    ...(toolUseEventId !== undefined ? { toolUseEventId, toolEvent: { kind: "tool" as const } } : {}),
    state: {
      status: "error" as const,
      input: { value: {}, preview: "{}", truncated: false },
      error: { type: "runtime_invalid_sequence", message: "invalid", retryable: false },
    },
    startedAt: now,
    completedAt: now,
    createdAt: now,
    updatedAt: now,
  };
}

export function buildRuntimeMessageProjectionMessage(
  id: string,
  role: "user" | "assistant",
  parts: readonly RuntimePart[],
): RuntimeMessage {
  const createdAt = "2026-06-14T00:00:00.000Z";
  return RuntimeMessageSchema.parse({
    id,
    sessionId: "session-a",
    role,
    origin: role === "user" ? "user" : "agent",
    sequence: role === "user" ? 0 : 1,
    status: "completed",
    createdAt,
    parts,
  });
}

export function buildSessionManagerBridgeRuntimeMessage(sessionId = "sesn_1", text = "bridge projected task notification"): DurableRuntimeMessage {
  const timestamp = "2026-06-14T00:00:00.000Z";
  return DurableRuntimeMessageSchema.parse({
    id: "msg_bridge_task_notification",
    sessionId,
    role: "user",
    origin: "runtime",
    sequence: 0,
    owningEventId: `sevt_${sessionId}_task_notification`,
    eventSequence: 1,
    status: "completed",
    createdAt: timestamp,
    updatedAt: timestamp,
    parts: [
      {
        id: "part_bridge_task_notification",
        sessionId,
        messageId: "msg_bridge_task_notification",
        sequence: 0,
        type: "text",
        text,
        truncated: false,
        status: "completed",
        createdAt: timestamp,
        updatedAt: timestamp,
        completedAt: timestamp,
      },
    ],
  });
}

export function buildSessionManagerColdUserMessage(sessionId: string): RuntimeMessage {
  const timestamp = "2026-06-14T00:00:00.000Z";
  return RuntimeMessageSchema.parse({
    id: `msg_${sessionId}`,
    sessionId,
    role: "user",
    origin: "user",
    sequence: 1,
    status: "completed",
    createdAt: timestamp,
    parts: [{
      id: `part_${sessionId}`,
      sessionId,
      messageId: `msg_${sessionId}`,
      sequence: 0,
      type: "text",
      text: "continue",
      truncated: false,
      status: "completed",
      createdAt: timestamp,
    }],
  });
}

export function buildSessionManagerReviewerDecisionMessage(sessionId: string, id: string, rationale: string): RuntimeMessage {
  const timestamp = "2026-06-14T00:00:00.000Z";
  return RuntimeMessageSchema.parse({
    id,
    sessionId,
    role: "assistant",
    origin: "agent",
    sequence: 1,
    status: "completed",
    createdAt: timestamp,
    parts: [{
      id: `part_${id}`,
      sessionId,
      messageId: id,
      sequence: 0,
      type: "text",
      text: JSON.stringify({
        risk_level: "low",
        user_authorization: "high",
        outcome: "allow",
        rationale,
      }),
      truncated: false,
      status: "completed",
      createdAt: timestamp,
      completedAt: timestamp,
    }],
  });
}

export function buildSessionRunHostUserMessage(id: string, sequence: number, text: string): RuntimeMessage {
  const createdAt = "2026-06-14T00:00:00.000Z";
  return RuntimeMessageSchema.parse({
    id,
    sessionId: "sesn_1",
    role: "user",
    origin: "user",
    sequence,
    status: "completed",
    createdAt,
    parts: [
      {
        id: `${id}-text`,
        sessionId: "sesn_1",
        messageId: id,
        sequence: 0,
        type: "text",
        text,
        truncated: false,
        status: "completed",
        createdAt,
      },
    ],
  });
}

export function buildSessionRunHostRuntimeNotificationMessage(sessionId: string): DurableRuntimeMessage {
  const createdAt = "2026-06-14T00:00:00.000Z";
  return DurableRuntimeMessageSchema.parse({
    id: "msg_bridge_task_notification",
    sessionId,
    role: "user",
    origin: "runtime",
    sequence: 0,
    owningEventId: `sevt_${sessionId}_task_notification`,
    eventSequence: 1,
    status: "completed",
    createdAt,
    updatedAt: createdAt,
    parts: [
      {
        id: "part_bridge_task_notification",
        sessionId,
        messageId: "msg_bridge_task_notification",
        sequence: 0,
        type: "text",
        text: "bridge projected task notification",
        truncated: false,
        status: "completed",
        createdAt,
        updatedAt: createdAt,
        completedAt: createdAt,
      },
    ],
  });
}

export function buildApprovalReviewerUserMessage(sessionId: string, messageId = "msg_user", text = "please edit the file"): RuntimeMessage {
  const createdAt = "2026-07-06T00:00:00.000Z";
  return DurableRuntimeMessageSchema.parse({
    id: messageId,
    sessionId,
    owningEventId: `evt_${messageId}`,
    eventSequence: 1,
    role: "user",
    origin: "user",
    sequence: 0,
    status: "completed",
    createdAt,
    parts: [{
      id: `part_${messageId}`,
      sessionId,
      messageId,
      sequence: 0,
      type: "text",
      text,
      truncated: false,
      status: "completed",
      createdAt,
      completedAt: createdAt,
    }],
  });
}

export function buildApprovalReviewerAssistantDraftMessage(sessionId: string): RuntimeMessage {
  const createdAt = "2026-07-06T00:00:00.000Z";
  return RuntimeMessageSchema.parse({
    id: "msg_assistant_draft",
    sessionId,
    role: "assistant",
    origin: "agent",
    sequence: 1,
    status: "streaming",
    createdAt,
    parts: [
      {
        id: "part_assistant_text",
        sessionId,
        messageId: "msg_assistant_draft",
        sequence: 0,
        type: "text",
        text: "I will update the file before calling the tool.",
        truncated: false,
        status: "streaming",
        createdAt,
        startedAt: createdAt,
      },
      {
        id: "part_assistant_reasoning",
        sessionId,
        messageId: "msg_assistant_draft",
        sequence: 1,
        type: "reasoning",
        providerPartId: "reasoning_1",
        text: "Need to patch one file.",
        truncated: false,
        status: "streaming",
        createdAt,
        startedAt: createdAt,
      },
      {
        id: "part_assistant_tool",
        sessionId,
        messageId: "msg_assistant_draft",
        sequence: 2,
        type: "tool",
        toolCallId: "tool_call_1",
        toolName: "Write",
        state: {
          status: "running",
          input: { value: { path: "src/a.ts", content: "ok" }, preview: "{\"path\":\"src/a.ts\",\"content\":\"ok\"}", truncated: false },
        },
        createdAt,
        startedAt: createdAt,
      },
    ],
  });
}

export function buildApprovalReviewerAssistantDecision(sessionId: string, outcome: "allow" | "deny", rationale: string): RuntimeMessage {
  return buildApprovalReviewerAssistantReviewerText(sessionId, JSON.stringify({ risk_level: "low", user_authorization: "high", outcome, rationale }));
}

export function buildApprovalReviewerAssistantReviewerText(sessionId: string, text: string): RuntimeMessage {
  const createdAt = "2026-07-06T00:00:00.000Z";
  return RuntimeMessageSchema.parse({
    id: "msg_reviewer",
    sessionId,
    role: "assistant",
    origin: "agent",
    sequence: 1,
    status: "completed",
    createdAt,
    parts: [{
      id: "part_reviewer",
      sessionId,
      messageId: "msg_reviewer",
      sequence: 0,
      type: "text",
      text,
      truncated: false,
      status: "completed",
      createdAt,
      completedAt: createdAt,
    }],
  });
}

export function buildBridgeClientRuntimeMessage(id: string, text: string): RuntimeMessage {
  return {
    id,
    sessionId: "sesn_1",
    role: "user",
    origin: "runtime",
    sequence: 0,
    status: "completed",
    createdAt: "2026-01-01T00:00:00.000Z",
    parts: [
      {
        id: `${id}_part`,
        sessionId: "sesn_1",
        messageId: id,
        sequence: 0,
        createdAt: "2026-01-01T00:00:00.000Z",
        type: "text",
        text,
        truncated: false,
        status: "completed",
      },
    ],
  };
}

export function buildBridgeClientDurableRuntimeMessage(id: string, text: string): RuntimeMessage {
  return DurableRuntimeMessageSchema.parse({
    ...buildBridgeClientRuntimeMessage(id, text),
    owningEventId: `evt_${id}`,
    eventSequence: 1,
  });
}

export function buildBridgeClientRuntimeRepairMessage(): RuntimeMessage {
  return {
    id: "msg_repair",
    sessionId: "sesn_1",
    role: "assistant",
    origin: "agent",
    sequence: 2,
    status: "completed",
    createdAt: "2026-01-01T00:00:00.000Z",
    parts: [
      {
        id: "part_repair",
        sessionId: "sesn_1",
        messageId: "msg_repair",
        sequence: 0,
        createdAt: "2026-01-01T00:00:00.000Z",
        type: "tool",
        toolCallId: "call_1",
        toolName: "unknown_tool",
        state: {
          status: "error",
          input: { value: { q: "x" }, preview: "{\"q\":\"x\"}", truncated: false },
          error: { type: "provider_tool_protocol_error", message: "invalid tool", retryable: false },
        },
        completedAt: "2026-01-01T00:00:00.000Z",
      },
    ],
  };
}

export function buildCoreHostsUserMessage(sessionId: string, id: string, sequence: number, text: string): DurableRuntimeMessage {
  const createdAt = "2026-06-16T00:00:00.000Z";
  return DurableRuntimeMessageSchema.parse({
    id,
    sessionId,
    owningEventId: `sevt_${id}`,
    eventSequence: sequence,
    role: "user",
    origin: "user",
    sequence,
    status: "completed",
    createdAt: "2026-06-16T00:00:00.000Z",
    parts: [
      {
        id: `${id}-text`,
        sessionId,
        messageId: id,
        sequence: 0,
        type: "text",
        text,
        truncated: false,
        status: "completed",
        createdAt: "2026-06-16T00:00:00.000Z",
      },
    ],
  });
}

export function buildCoreHostsAssistantRunningToolMessage(
  sessionId: string,
  id: string,
  sequence: number,
  toolCallId: string,
  toolName: string,
  toolUseEventId: string,
  input: unknown,
  toolEvent: { readonly kind: "tool" } | {
    readonly kind: "mcp";
    readonly mcpServerName: string;
  } = { kind: "tool" },
): DurableRuntimeMessage {
  const createdAt = "2026-06-16T00:00:00.000Z";
  return DurableRuntimeMessageSchema.parse({
    id,
    sessionId,
    owningEventId: toolUseEventId,
    eventSequence: sequence,
    role: "assistant",
    origin: "agent",
    sequence,
    status: "completed",
    createdAt: "2026-06-16T00:00:00.000Z",
    parts: [
      {
        id: `${id}-tool`,
        sessionId,
        messageId: id,
        sequence: 0,
        type: "tool",
        toolCallId,
        toolName,
        toolUseEventId,
        toolEvent,
        state: {
          status: "running",
          input: {
            value: input,
            preview: JSON.stringify(input),
            truncated: false,
          },
        },
        startedAt: "2026-06-16T00:00:00.000Z",
        createdAt: "2026-06-16T00:00:00.000Z",
      },
    ],
  });
}

export function buildCoreHostsBridgeRuntimeMessage(sessionId: string, text: string): RuntimeMessage {
  const createdAt = "2026-06-16T00:00:00.000Z";
  return RuntimeMessageSchema.parse({
    id: `msg_${sessionId}_task_notification`,
    sessionId,
    role: "user",
    origin: "runtime",
    sequence: 0,
    status: "completed",
    createdAt: "2026-06-16T00:00:00.000Z",
    parts: [
      {
        id: `part_${sessionId}_task_notification`,
        sessionId,
        messageId: `msg_${sessionId}_task_notification`,
        sequence: 0,
        type: "text",
        text,
        truncated: false,
        status: "completed",
        createdAt: "2026-06-16T00:00:00.000Z",
      },
    ],
  });
}

export function buildCoreHostsDurableBridgeRuntimeMessage(sessionId: string, text: string): DurableRuntimeMessage {
  return DurableRuntimeMessageSchema.parse({
    ...buildCoreHostsBridgeRuntimeMessage(sessionId, text),
    sequence: 1,
    owningEventId: `sevt_${sessionId}_task_notification`,
    eventSequence: 1,
  });
}

export function buildRuntimeServiceBridgeRuntimeMessage(options: { readonly text?: string; readonly sessionId?: string } = {}): RuntimeMessage {
  const sessionId = options.sessionId ?? "sesn_1";
  const now = "2026-01-01T00:00:00.000Z";
  return RuntimeMessageSchema.parse({
    id: "msg_bridge_task_notification",
    sessionId,
    role: "user",
    origin: "runtime",
    sequence: 0,
    status: "completed",
    createdAt: now,
    updatedAt: now,
    parts: [
      {
        id: "part_bridge_task_notification",
        sessionId,
        messageId: "msg_bridge_task_notification",
        sequence: 0,
        type: "text",
        text: options.text ?? "bridge projected task notification",
        truncated: false,
        status: "completed",
        createdAt: now,
        updatedAt: now,
        completedAt: now,
      },
    ],
  });
}

export function buildRuntimeServiceDurableBridgeRuntimeMessage(
  options: { readonly text?: string; readonly sessionId?: string } = {},
): DurableRuntimeMessage {
  const message = buildRuntimeServiceBridgeRuntimeMessage(options);
  return DurableRuntimeMessageSchema.parse({
    ...message,
    owningEventId: `sevt_${message.sessionId}_task_notification`,
    eventSequence: 1,
  });
}

export function buildToolRunnerCompletedToolMessage(toolName: string, input: RuntimeJsonValue, output: string): RuntimeMessage {
  return {
    id: `msg_${toolName.toLowerCase()}_completed`,
    sessionId: "sesn_1",
    role: "assistant",
    origin: "agent",
    sequence: 0,
    status: "completed",
    createdAt: "2026-01-01T00:00:00.000Z",
    updatedAt: "2026-01-01T00:00:00.000Z",
    parts: [{
      id: `part_${toolName.toLowerCase()}_completed`,
      sessionId: "sesn_1",
      messageId: `msg_${toolName.toLowerCase()}_completed`,
      sequence: 0,
      type: "tool",
      toolCallId: `tool_${toolName.toLowerCase()}_completed`,
      toolName,
      toolUseEventId: `evt_${toolName.toLowerCase()}_completed`,
      toolEvent: { kind: "tool" },
      state: {
        status: "completed",
        input: {
          value: input,
          preview: JSON.stringify(input),
          truncated: false,
        },
        output: {
          text: output,
          truncated: false,
        },
      },
      startedAt: "2026-01-01T00:00:00.000Z",
      completedAt: "2026-01-01T00:00:00.000Z",
      createdAt: "2026-01-01T00:00:00.000Z",
      updatedAt: "2026-01-01T00:00:00.000Z",
    }],
  };
}

export function buildToolRunnerRuntimeTextMessage(
  id: string,
  role: "user" | "assistant",
  origin: "user" | "agent" | "runtime",
  sequence: number,
  text: string,
): RuntimeMessage {
  const createdAt = "2026-01-01T00:00:00.000Z";
  return RuntimeMessageSchema.parse({
    id,
    sessionId: "sesn_1",
    role,
    origin,
    sequence,
    status: "completed",
    createdAt,
    parts: [{
      id: `${id}_part`,
      sessionId: "sesn_1",
      messageId: id,
      sequence: 0,
      type: "text",
      text,
      truncated: false,
      status: "completed",
      createdAt,
      completedAt: createdAt,
    }],
  });
}
