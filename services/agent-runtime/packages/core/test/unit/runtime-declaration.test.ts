import { describe, expect, test } from "bun:test";
import {
  acceptedInputDrafts,
  applyAcceptedInputReceipt,
  applyInterruptInputReceipt,
  applyRuntimeRequestEndReceipt,
  applyTaskNotificationReceipt,
  applyToolConfirmationReceipt,
  runtimeTerminationToolDeclarations,
  taskNotificationDraft,
  taskNotificationSourceId,
} from "../../src/runtime/runtime-declaration.js";
import {
  DurableRuntimeMessageSchema,
  RuntimeMessageDraftSchema,
  RuntimePartDraftSchema,
} from "../../src/contracts/runtime.js";
import { stableRuntimeID } from "../../src/runtime/runtime-identity.js";
import { acceptedInputReceipt } from "./runtime-declaration-fixtures.js";
import { buildRuntimeControlCommitResult } from "./runtime-message-builders.js";
import type {
  RuntimeAcceptedInputState,
  RuntimePendingApprovalToolJobState,
} from "../../src/thread-loop/thread-state.js";

describe("runtime declaration identity and message shapes", () => {
  test("derives the shared framed stable ids", () => {
    const messageID = stableRuntimeID(
      "runtime_message_draft",
      "ws_1",
      "ses_1",
      "thr_1",
      "messages",
      "rin_1",
      "user_message",
      "0",
    );

    expect(messageID).toBe(
      "stid_edcc6aa88ddf349e058ade77c0f169d73c6f3c0343d99cef1adc640fd486d82e",
    );
    expect(stableRuntimeID("runtime_message_part_draft", messageID, "text", "0")).toBe(
      "stid_080e946f2be6e403aaaeb2afab207746171d95850660f612af67de33f1ed5828",
    );
  });

  test("applies one interrupt message with multiple pending-tool receipt pairs", () => {
    const scope = {
      requestId: "req_interrupt_pairs",
      workspaceId: "ws_1",
      sessionId: "ses_1",
      sessionThreadId: "thr_1",
      bindingId: "bind_1",
      bindingGeneration: 1,
      targetPodUid: "pod_1",
      runtimeInputId: "rin_interrupt_pairs",
      eventIds: ["sevt_interrupt_pairs"],
      sequenceFrom: 7,
      sequenceTo: 7,
    };
    const runtimeLocalId = "stid_interrupt_pairs";
    const draft = RuntimeMessageDraftSchema.parse({
      runtimeLocalId,
      sourceKind: "interrupt_control",
      sourceId: scope.runtimeInputId,
      sourceEventId: scope.eventIds[0],
      draftKind: "cancellation",
      ordinal: 0,
      role: "assistant",
      origin: "agent",
      status: "completed",
      parts: ["sevt_tool_1", "sevt_tool_2"].map((toolUseEventId, ordinal) => ({
        runtimeLocalPartId: `stid_interrupt_part_${ordinal}`,
        ordinal,
        type: "tool" as const,
        toolCallId: `call_${ordinal}`,
        toolName: "Write",
        toolUseEventId,
        state: {
          status: "cancelled" as const,
          error: { type: "runtime" as const, message: "aborted", retryable: false },
        },
        completedAt: "2026-07-30T00:00:00.000Z",
      })),
    });
    const declaration = {
      drafts: [draft],
      pendingToolCancellations: ["sevt_tool_1", "sevt_tool_2"].map((toolUseEventId) => ({
        toolUseEventId,
        runtimeLocalId,
      })),
      sandboxExecutionToolUseEventIds: [],
    };
    const committed = buildRuntimeControlCommitResult(scope, "interrupt_control", declaration);
    if (!committed.ok || !("receipt" in committed)) {
      throw new Error("test interrupt receipt is missing");
    }

    const messages = applyInterruptInputReceipt({
      sessionId: scope.sessionId,
      sessionThreadId: scope.sessionThreadId,
      runtimeInputId: scope.runtimeInputId,
      eventIds: scope.eventIds,
      drafts: declaration.drafts,
      pendingToolCancellations: declaration.pendingToolCancellations,
      sandboxExecutionToolUseEventIds: declaration.sandboxExecutionToolUseEventIds,
      existingMessages: [],
    }, committed.receipt);

    expect(messages).toHaveLength(1);
    expect(messages[0]?.parts).toHaveLength(2);
  });

  test("keeps drafts unstamped and durable messages database stamped", () => {
    const part = RuntimePartDraftSchema.parse({
      runtimeLocalPartId: "stid_part",
      type: "text",
      ordinal: 0,
      text: "hello",
      truncated: false,
      status: "completed",
    });
    expect(RuntimeMessageDraftSchema.parse({
      runtimeLocalId: "stid_message",
      sourceKind: "messages",
      sourceId: "rin_1",
      sourceEventId: "sevt_1",
      draftKind: "user_input",
      ordinal: 0,
      role: "user",
      origin: "user",
      status: "completed",
      parts: [part],
    })).not.toHaveProperty("id");

    expect(DurableRuntimeMessageSchema.parse({
      id: "msg_1",
      sessionId: "ses_1",
      owningEventId: "sevt_1",
      eventSequence: 7,
      role: "user",
      origin: "user",
      sequence: 4,
      status: "completed",
      createdAt: "2026-07-29T00:00:00.000Z",
      parts: [{
        id: "part_1",
        sessionId: "ses_1",
        messageId: "msg_1",
        sequence: 0,
        createdAt: "2026-07-29T00:00:00.000Z",
        type: "text",
        text: "hello",
        truncated: false,
        status: "completed",
      }],
    })).toMatchObject({
      id: "msg_1",
      owningEventId: "sevt_1",
      eventSequence: 7,
    });
  });

  test("reapplying one approval receipt keeps a single durable projection", () => {
    const draft = RuntimeMessageDraftSchema.parse({
      runtimeLocalId: "stid_approval_message",
      sourceKind: "tool_confirmation",
      sourceId: "rin_approval",
      sourceEventId: "sevt_approval",
      draftKind: "approval_input",
      ordinal: 0,
      role: "user",
      origin: "user",
      status: "completed",
      parts: [{
        runtimeLocalPartId: "stid_approval_part",
        ordinal: 0,
        type: "text",
        text: "Approval allowed",
        truncated: false,
        status: "completed",
      }],
    });
    const receipt = {
      sessionThreadId: "thr_1",
      operationKind: "commit_inputs",
      sourceKind: "tool_confirmation",
      sourceId: "rin_approval",
      declarationDigest: "digest_approval",
      events: [{
        sessionThreadId: "thr_1",
        sourceEventId: "sevt_approval",
        eventId: "sevt_approval",
        eventSequence: 5,
        disposition: "existing" as const,
      }],
      messages: [{
        runtimeLocalId: draft.runtimeLocalId,
        sessionThreadId: "thr_1",
        owningEventId: "sevt_approval",
        messageId: "msg_approval",
        messageSequence: 3,
        createdAt: "2026-07-30T00:00:00.000Z",
        updatedAt: "2026-07-30T00:00:00.000Z",
        disposition: "created" as const,
        parts: [{
          runtimeLocalPartId: draft.parts[0]!.runtimeLocalPartId,
          partId: "part_approval",
          messageId: "msg_approval",
          partSequence: 0,
          createdAt: "2026-07-30T00:00:00.000Z",
          updatedAt: "2026-07-30T00:00:00.000Z",
          disposition: "created" as const,
        }],
      }],
      pendingAttachmentDelta: [],
      pendingToolDelta: [],
      prefixConsumptions: [],
      childLifecycle: [],
    };
    const application = {
      sessionId: "ses_1",
      sessionThreadId: "thr_1",
      runtimeInputId: "rin_approval",
      sourceEventId: "sevt_approval",
      draft,
    } as const;
    const first = applyToolConfirmationReceipt({
      ...application,
      existingMessages: [],
    }, receipt);
    const replay = applyToolConfirmationReceipt({
      ...application,
      existingMessages: first,
    }, receipt);

    expect(first).toHaveLength(1);
    expect(replay).toEqual(first);
  });

  test("authors a deterministic task notification and applies only its database receipt", () => {
    const payloadJson = JSON.stringify({
      task_id: "task_1",
      source_tool_use_event_id: "sevt_tool_1",
      status: "completed",
      stdout: { text: "done", truncated: false },
      stderr: { text: "", truncated: false },
    });
    const draft = taskNotificationDraft({
      workspaceId: "ws_1",
      sessionId: "ses_1",
      sessionThreadId: "thr_1",
      runtimeInputId: "rin_task_1",
      taskId: "task_1",
      payloadJson,
    });
    const sourceId = taskNotificationSourceId("rin_task_1", "task_1");
    expect(taskNotificationDraft({
      workspaceId: "ws_1",
      sessionId: "ses_1",
      sessionThreadId: "thr_1",
      runtimeInputId: "rin_task_1",
      taskId: "task_1",
      payloadJson,
    })).toEqual(draft);
    expect(draft).toMatchObject({
      sourceKind: "task_notification",
      sourceId,
      draftKind: "task_notification",
      role: "user",
      origin: "runtime",
      status: "completed",
      parts: [{ type: "text", text: payloadJson, truncated: false, status: "completed" }],
    });

    const messages = applyTaskNotificationReceipt({
      sessionId: "ses_1",
      sessionThreadId: "thr_1",
      sourceId,
      draft,
      existingMessages: [],
    }, {
      sessionThreadId: "thr_1",
      operationKind: "commit_task_notification_result",
      sourceKind: "task_notification",
      sourceId,
      declarationDigest: "digest_1",
      events: [{
        sessionThreadId: "thr_1",
        sourceEventId: sourceId,
        eventId: "sevt_task_1",
        eventSequence: 9,
        disposition: "created",
      }],
      messages: [{
        runtimeLocalId: draft.runtimeLocalId,
        sessionThreadId: "thr_1",
        owningEventId: "sevt_task_1",
        messageId: "msg_task_1",
        messageSequence: 6,
        createdAt: "2026-07-29T00:00:00.000Z",
        updatedAt: "2026-07-29T00:00:00.000Z",
        disposition: "created",
        parts: [{
          runtimeLocalPartId: draft.parts[0]!.runtimeLocalPartId,
          partId: "part_task_1",
          messageId: "msg_task_1",
          partSequence: 0,
          createdAt: "2026-07-29T00:00:00.000Z",
          updatedAt: "2026-07-29T00:00:00.000Z",
          disposition: "created",
        }],
      }],
      pendingAttachmentDelta: [],
      pendingToolDelta: [],
      prefixConsumptions: [],
      childLifecycle: [],
    });
    expect(messages).toEqual([expect.objectContaining({
      id: "msg_task_1",
      owningEventId: "sevt_task_1",
      eventSequence: 9,
      sequence: 6,
      parts: [expect.objectContaining({ id: "part_task_1", text: payloadJson })],
    })]);
  });

  test("closes draft part kinds and rejects message routing metadata", () => {
    for (const part of [
      { runtimeLocalPartId: "p_text", type: "text", ordinal: 0, text: "text", truncated: false, status: "completed" },
      { runtimeLocalPartId: "p_reasoning", type: "reasoning", ordinal: 0, text: "reasoning", truncated: false, status: "completed" },
      {
        runtimeLocalPartId: "p_tool",
        type: "tool",
        ordinal: 0,
        toolCallId: "call_1",
        toolName: "Read",
        state: { status: "pending" },
      },
      { runtimeLocalPartId: "p_start", type: "step-start", ordinal: 0 },
      { runtimeLocalPartId: "p_finish", type: "step-finish", ordinal: 0, finishReason: "stop" },
    ] as const) {
      expect(RuntimePartDraftSchema.safeParse(part).success).toBe(true);
    }
    expect(RuntimePartDraftSchema.safeParse({
      runtimeLocalPartId: "p_unknown",
      type: "unknown",
      ordinal: 0,
    }).success).toBe(false);

    const routed = {
      id: "msg_1",
      sessionId: "ses_1",
      owningEventId: "sevt_1",
      eventSequence: 1,
      role: "user",
      origin: "user",
      sequence: 1,
      status: "completed",
      createdAt: "2026-07-29T00:00:00.000Z",
      providerId: "openai",
      modelId: "gpt-5.5",
      parts: [],
    };
    expect(DurableRuntimeMessageSchema.safeParse(routed).success).toBe(false);
  });

  test("applies only a complete receipt for the exact accepted-input declaration", () => {
    const input: RuntimeAcceptedInputState = {
      requestId: "req_1",
      workspaceId: "ws_1",
      sessionId: "ses_1",
      sessionThreadId: "thr_1",
      bindingId: "bind_1",
      bindingGeneration: 1,
      targetPodUid: "pod_1",
      runtimeInputId: "rin_1",
      eventIds: ["sevt_1"],
      sequenceFrom: 7,
      sequenceTo: 7,
      kind: "messages",
      payloadJson: JSON.stringify({
        messages: [{
          id: "temporary",
          sessionId: "ses_1",
          role: "user",
          origin: "user",
          sequence: 0,
          status: "completed",
          createdAt: "2026-07-29T00:00:00.000Z",
          parts: [{
            id: "temporary_part",
            sessionId: "ses_1",
            messageId: "temporary",
            sequence: 0,
            createdAt: "2026-07-29T00:00:00.000Z",
            type: "text",
            text: "hello",
            truncated: false,
            status: "completed",
          }],
        }],
      }),
    };
    const drafts = acceptedInputDrafts(input);
    const receipt = acceptedInputReceipt(input).receipt;
    expect(applyAcceptedInputReceipt(input, drafts, receipt)).toHaveLength(1);
    expect(() => applyAcceptedInputReceipt(input, drafts, {
      ...receipt,
      messages: [...receipt.messages, receipt.messages[0]!],
    })).toThrow("duplicate message stamp");
  });

  test("applies a reviewer input receipt whose internal source event was created with its message", () => {
    const prompt = {
      id: "msg_reviewer_prompt",
      sessionId: "ses_1",
      role: "user" as const,
      origin: "runtime" as const,
      sequence: 0,
      status: "completed" as const,
      createdAt: "2026-08-04T00:00:00.000Z",
      parts: [{
        id: "part_reviewer_prompt",
        sessionId: "ses_1",
        messageId: "msg_reviewer_prompt",
        sequence: 0,
        type: "text" as const,
        text: "review this tool call",
        truncated: false,
        status: "completed" as const,
        createdAt: "2026-08-04T00:00:00.000Z",
      }],
    };
    const input: RuntimeAcceptedInputState = {
      requestId: "req_reviewer",
      workspaceId: "ws_1",
      sessionId: "ses_1",
      sessionThreadId: "thr_reviewer",
      bindingId: "bind_1",
      bindingGeneration: 1,
      targetPodUid: "pod_1",
      runtimeInputId: "rin_reviewer",
      eventIds: ["sevt_reviewer"],
      sequenceFrom: 0,
      sequenceTo: 0,
      kind: "approval_review",
      reviewId: "arvw_1",
      parentThreadId: "thr_main",
      targetModelToolCallId: "call_1",
      targetToolName: "Write",
      promptItems: [prompt],
      outputSchemaJson: "{}",
    };
    const drafts = acceptedInputDrafts(input);
    const receipt = acceptedInputReceipt(input).receipt;

    expect(receipt.events).toEqual([
      expect.objectContaining({
        eventId: "sevt_reviewer",
        sourceEventId: "sevt_reviewer",
        disposition: "created",
      }),
    ]);
    expect(applyAcceptedInputReceipt(input, drafts, receipt)).toHaveLength(1);
  });

  test("authors one fixed assistant rejection draft for every source in a rejected batch", () => {
    const input: RuntimeAcceptedInputState = {
      requestId: "req_rejection",
      workspaceId: "ws_1",
      sessionId: "ses_1",
      sessionThreadId: "thr_1",
      bindingId: "bind_1",
      bindingGeneration: 1,
      targetPodUid: "pod_1",
      runtimeInputId: "rin_rejection",
      eventIds: ["sevt_rejected_input_1", "sevt_rejected_input_2"],
      sequenceFrom: 8,
      sequenceTo: 9,
      kind: "rejection",
      reasonCode: "runtime_command_payload_too_large",
    };

    const drafts = acceptedInputDrafts(input);
    expect(drafts).toEqual([
      expect.objectContaining({
        sourceKind: "rejection",
        sourceId: "rin_rejection",
        sourceEventId: "sevt_rejected_input_1",
        draftKind: "rejection",
        role: "assistant",
        origin: "agent",
        status: "completed",
        parts: [
          expect.objectContaining({
            type: "text",
            text: "The session runtime could not accept this input.",
          }),
        ],
      }),
      expect.objectContaining({
        sourceEventId: "sevt_rejected_input_2",
        draftKind: "rejection",
        parts: [expect.objectContaining({
          type: "text",
          text: "The session runtime could not accept this input.",
        })],
      }),
    ]);
    expect(new Set(drafts.map((draft) => draft.runtimeLocalId)).size).toBe(2);
  });

  test("request-end receipt preserves an earlier durable repair when the terminal seal is empty", () => {
    const repair = DurableRuntimeMessageSchema.parse({
      id: "msg_repair",
      sessionId: "ses_1",
      owningEventId: "sevt_repair",
      eventSequence: 7,
      role: "assistant",
      origin: "agent",
      sequence: 4,
      status: "completed",
      createdAt: "2026-07-29T00:00:00.000Z",
      parts: [{
        id: "part_repair",
        sessionId: "ses_1",
        messageId: "msg_repair",
        sequence: 0,
        createdAt: "2026-07-29T00:00:00.000Z",
        type: "tool",
        toolCallId: "call_repair",
        toolName: "Bash",
        state: {
          status: "error",
          error: {
            type: "runtime",
            message: "The provider emitted an unsupported internal tool call.",
            retryable: false,
          },
        },
        completedAt: "2026-07-29T00:00:00.000Z",
      }],
    });

    expect(applyRuntimeRequestEndReceipt({
      sessionId: "ses_1",
      sessionThreadId: "thr_1",
      modelRequestId: "mrq_1",
      eventId: "sevt_request_end",
      drafts: [],
      existingMessages: [repair],
    }, {
      sessionThreadId: "thr_1",
      operationKind: "write_request_end",
      sourceKind: "model_request",
      sourceId: "mrq_1",
      declarationDigest: "digest_request_end",
      pendingAttachmentDelta: [],
      pendingToolDelta: [],
      prefixConsumptions: [],

      childLifecycle: [],
      events: [{
        sessionThreadId: "thr_1",
        sourceEventId: "mrq_1",
        eventId: "sevt_request_end",
        eventSequence: 8,
        disposition: "created",
      }],
      messages: [],
    })).toEqual([repair]);
  });

  test("request-end receipt can seal only the existing assistant message and its part associations", () => {
    const existing = DurableRuntimeMessageSchema.parse({
      id: "msg_assistant",
      sessionId: "ses_1",
      owningEventId: "sevt_assistant",
      eventSequence: 7,
      role: "assistant",
      origin: "agent",
      sequence: 4,
      status: "streaming",
      createdAt: "2026-07-29T00:00:00.000Z",
      parts: [
        {
          id: "part_text_1",
          sessionId: "ses_1",
          messageId: "msg_assistant",
          sequence: 0,
          createdAt: "2026-07-29T00:00:00.000Z",
          type: "text",
          text: "first",
          truncated: false,
          status: "completed",
        },
        {
          id: "part_text_2",
          sessionId: "ses_1",
          messageId: "msg_assistant",
          sequence: 1,
          createdAt: "2026-07-29T00:00:00.000Z",
          type: "text",
          text: "second",
          truncated: false,
          status: "completed",
        },
      ],
    });
    const runtimeLocalId = stableRuntimeID(
      "runtime_message_draft",
      "ws_1",
      "ses_1",
      "thr_1",
      "model_request",
      "mrq_1",
      "assistant_text",
      "0",
    );
    const draft = RuntimeMessageDraftSchema.parse({
      runtimeLocalId,
      sourceKind: "model_request",
      sourceId: "mrq_1",
      draftKind: "assistant_text",
      ordinal: 0,
      role: "assistant",
      origin: "agent",
      status: "completed",
      parts: existing.parts.map((part, ordinal) => ({
        runtimeLocalPartId: stableRuntimeID(
          "runtime_message_part_draft",
          runtimeLocalId,
          "text",
          String(ordinal),
        ),
        ordinal,
        type: "text" as const,
        text: part.type === "text" ? part.text : "",
        truncated: false,
        status: "completed" as const,
      })),
    });
    const receipt = {
      sessionThreadId: "thr_1",
      operationKind: "write_request_end",
      sourceKind: "model_request",
      sourceId: "mrq_1",
      declarationDigest: "digest_request_end",
      pendingAttachmentDelta: [],
      pendingToolDelta: [],
      prefixConsumptions: [],
      childLifecycle: [],
      events: [{
        sessionThreadId: "thr_1",
        sourceEventId: "mrq_1",
        eventId: "sevt_request_end",
        eventSequence: 8,
        disposition: "created" as const,
      }],
      messages: [{
        runtimeLocalId,
        sessionThreadId: "thr_1",
        owningEventId: "sevt_assistant",
        messageId: "msg_assistant",
        messageSequence: 4,
        createdAt: "2026-07-29T00:00:00.000Z",
        updatedAt: "2026-07-29T00:01:00.000Z",
        disposition: "updated" as const,
        parts: draft.parts.map((part, index) => ({
          runtimeLocalPartId: part.runtimeLocalPartId,
          partId: `part_text_${index + 1}`,
          messageId: "msg_assistant",
          partSequence: index,
          createdAt: "2026-07-29T00:00:00.000Z",
          updatedAt: "2026-07-29T00:01:00.000Z",
          disposition: "updated" as const,
        })),
      }],
    };
    const input = {
      sessionId: "ses_1",
      sessionThreadId: "thr_1",
      modelRequestId: "mrq_1",
      eventId: "sevt_request_end",
      drafts: [draft],
      existingMessages: [existing],
      expectedMessage: existing,
    };

    expect(applyRuntimeRequestEndReceipt(input, receipt)[0]).toMatchObject({
      id: "msg_assistant",
      owningEventId: "sevt_assistant",
      status: "completed",
    });
    expect(() => applyRuntimeRequestEndReceipt(input, {
      ...receipt,
      messages: [{ ...receipt.messages[0]!, messageId: "msg_replacement", disposition: "created" }],
    })).toThrow("replaced its durable assistant message");
    expect(() => applyRuntimeRequestEndReceipt(input, {
      ...receipt,
      messages: [{
        ...receipt.messages[0]!,
        parts: [
          { ...receipt.messages[0]!.parts[0]!, partId: "part_text_2" },
          { ...receipt.messages[0]!.parts[1]!, partId: "part_text_1" },
        ],
      }],
    })).toThrow("changed a durable part association");
  });

  test("freezes current-thread pending tools into deterministic termination declarations", () => {
    const assistantMessage = DurableRuntimeMessageSchema.parse({
      id: "msg_pending",
      sessionId: "ses_1",
      owningEventId: "sevt_tool_use",
      eventSequence: 8,
      role: "assistant",
      origin: "agent",
      sequence: 5,
      status: "streaming",
      createdAt: "2026-07-29T00:00:00.000Z",
      parts: [{
        id: "part_pending",
        sessionId: "ses_1",
        messageId: "msg_pending",
        sequence: 0,
        createdAt: "2026-07-29T00:00:00.000Z",
        type: "tool",
        toolCallId: "call_pending",
        toolName: "Bash",
        toolUseEventId: "sevt_tool_use",
        state: {
          status: "running",
          input: { value: { command: "sleep 10" }, preview: "{\"command\":\"sleep 10\"}", truncated: false },
        },
      }],
    });
    const toolPart = assistantMessage.parts[0]!;
    if (toolPart.type !== "tool") {
      throw new Error("test fixture tool part is invalid");
    }
    const pending = {
      toolUseEventId: "sevt_tool_use",
      modelRequestId: "mreq_1",
      source: { providerId: "fake", modelId: "fake-chat" },
      assistantMessage,
      toolPart,
      job: {
        id: "mreq_1:call_pending",
        modelOrder: 0,
        modelToolCallId: "call_pending",
        kind: "builtin",
        name: "Bash",
        route: { kind: "gateway", operation: "RunWeb" },
        input: { command: "sleep 10" },
        runPolicy: { mode: "exclusive", conflictKeys: ["Bash"] },
        gateState: "waiting_approval",
        approvalSource: "user",
      },
      entry: {
        name: "Bash",
        definition: { name: "Bash", description: "Run a command", inputSchema: { type: "object" } },
        route: { kind: "gateway", operation: "RunWeb" },
        formatter: { successShape: "text", errorShape: "text", forbiddenFields: [] },
        defaultPermissionPolicy: "always_ask",
        required: false,
      },
      committedMessages: [assistantMessage],
    } satisfies RuntimePendingApprovalToolJobState;
    const failure = {
      type: "runtime",
      code: "runtime_invalid_sequence",
      message: "Runtime operation failed.",
      retryable: false,
      fatal: true,
      reason: "runtime_contract_validation",
      retryStatus: { type: "terminal" },
    } as const;

    const first = runtimeTerminationToolDeclarations({
      workspaceId: "ws_1",
      sessionId: "ses_1",
      sessionThreadId: "thr_1",
      durableTurnId: "sevt_turn",
      pendingTools: [pending],
      failure,
      completedAt: "2026-07-29T00:01:00.000Z",
    });
    const replay = runtimeTerminationToolDeclarations({
      workspaceId: "ws_1",
      sessionId: "ses_1",
      sessionThreadId: "thr_1",
      durableTurnId: "sevt_turn",
      pendingTools: [pending],
      failure,
      completedAt: "2026-07-29T00:01:00.000Z",
    });

    expect(replay).toEqual(first);
    expect(first.pendingToolCancellations).toEqual([{
      toolUseEventId: "sevt_tool_use",
      runtimeLocalId: first.drafts[0]!.runtimeLocalId,
    }]);
    expect(first.drafts[0]).toMatchObject({
      sourceKind: "runtime_termination",
      sourceId: "sevt_turn",
      draftKind: "termination",
      role: "assistant",
      origin: "agent",
      status: "completed",
      parts: [{
        type: "tool",
        toolUseEventId: "sevt_tool_use",
        state: {
          status: "cancelled",
          error: { message: "Runtime operation failed." },
        },
      }],
    });
  });
});
