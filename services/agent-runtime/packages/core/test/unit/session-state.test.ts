import { describe, expect, test } from "bun:test";
import { RuntimeMessageSchema } from "../../src/contracts/runtime.js";
import type {
  RuntimeAcceptedInputState,
  RuntimeTaskNotificationState,
} from "../../src/session/session-state.js";
import { MaxProviderRequestAttachments, SessionState } from "../../src/session/session-state.js";

const timestamp = "2026-01-01T00:00:00.000Z";

describe("SessionState", () => {
  test("successful request hints stay paired and an actual model change invalidates both", () => {
    const state = new SessionState("sesn_compaction_hints");
    state.updateCurrentModel({ providerId: "openai", modelId: "gpt-5.5" });
    state.recordLastRequestCompletion({
      inputTokens: 300_000,
      outputTokens: 2_000,
      reasoningTokens: 50_000,
      cacheReadTokens: 1_000,
      cacheWriteTokens: 500,
    }, {
      contextWindowTokens: 400_000,
      inputLimitTokens: 272_000,
      outputTokenLimit: 128_000,
    }, 41);

    state.updateCurrentModel({ providerId: "openai", modelId: "gpt-5.5" });
    expect(state.lastRequestUsage()).toMatchObject({ inputTokens: 300_000 });
    expect(state.lastRequestModelLimits()).toEqual({
      contextWindowTokens: 400_000,
      inputLimitTokens: 272_000,
      outputTokenLimit: 128_000,
    });
    expect(state.lastRequestContextAnchorSequence()).toBe(41);

    state.updateCurrentModel({ providerId: "anthropic", modelId: "claude-fable-5" });
    expect(state.lastRequestUsage()).toBeUndefined();
    expect(state.lastRequestModelLimits()).toBeUndefined();
    expect(state.lastRequestContextAnchorSequence()).toBeUndefined();
  });

  test("caps pending provider attachments and records model-visible overflow work", () => {
    const state = new SessionState("sesn_attachments");
    state.addPendingAttachments(Array.from({ length: MaxProviderRequestAttachments + 3 }, (_, index) => ({
      transient: {
        attachmentRef: `att_${index}`,
        sourceToolUseEventId: `sevt_tool_${index}`,
        sourcePath: `/tmp/image-${index}.png`,
        pageRange: "",
        detail: "auto",
      },
      fileBacked: undefined,
      mime: "image/png",
      filename: `image-${index}.png`,
    })));

    expect(state.pendingAttachments()).toHaveLength(MaxProviderRequestAttachments);
    expect(state.pendingAttachments().at(-1)?.transient?.attachmentRef).toBe("att_31");
    expect(state.takePendingAttachmentOverflowCount()).toBe(3);
    expect(state.takePendingAttachmentOverflowCount()).toBe(0);
  });

  test("owns pending attachment origins independently of caller snapshots", () => {
    const state = new SessionState("sesn_attachment_ownership");
    const attachment = {
      transient: {
        attachmentRef: "att_original",
        sourceToolUseEventId: "sevt_tool_1",
        sourcePath: "/tmp/image.png",
        pageRange: "",
        detail: "auto",
      },
      fileBacked: undefined,
      mime: "image/png",
      filename: "image.png",
    };

    state.addPendingAttachments([attachment]);
    attachment.transient.attachmentRef = "att_mutated_input";
    const snapshot = state.pendingAttachments();
    snapshot[0]!.transient!.attachmentRef = "att_mutated_output";

    expect(state.pendingAttachments()[0]?.transient?.attachmentRef).toBe("att_original");
  });

  test("keeps a full active ride separate from attachments queued for the next request", () => {
    const state = new SessionState("sesn_attachment_rides");
    const activeRide = Array.from({ length: MaxProviderRequestAttachments }, (_, index) => ({
      transient: {
        attachmentRef: `att_active_${index}`,
        sourceToolUseEventId: `sevt_active_${index}`,
        sourcePath: `/tmp/active-${index}.png`,
        pageRange: "",
        detail: "auto" as const,
      },
      fileBacked: undefined,
      mime: "image/png",
      filename: `active-${index}.png`,
    }));
    const nextRide = {
      transient: {
        attachmentRef: "att_next",
        sourceToolUseEventId: "sevt_next",
        sourcePath: "/tmp/next.png",
        pageRange: "",
        detail: "auto" as const,
      },
      fileBacked: undefined,
      mime: "image/png",
      filename: "next.png",
    };

    state.addPendingAttachments(activeRide);
    expect(state.beginPendingAttachmentRide()).toEqual(activeRide);
    state.addPendingAttachments([nextRide]);
    state.reconcilePendingAttachments([...activeRide, nextRide]);
    expect(state.pendingAttachments()).toEqual([...activeRide, nextRide]);
    expect(state.takePendingAttachmentOverflowCount()).toBe(0);

    state.settlePendingAttachmentRide();
    expect(state.beginPendingAttachmentRide()).toEqual([nextRide]);
  });

  test("agent-mail delivery identity deduplicates hot enqueue and cold resolution after ACK", () => {
    const state = new SessionState("sesn_agent_mail_dedup");
    const message = RuntimeMessageSchema.parse({
      id: "msg_agent_mail",
      sessionId: "sesn_agent_mail_dedup",
      role: "user",
      origin: "runtime",
      sequence: 0,
      status: "completed",
      createdAt: timestamp,
      updatedAt: timestamp,
      parts: [{
        id: "part_agent_mail",
        sessionId: "sesn_agent_mail_dedup",
        messageId: "msg_agent_mail",
        sequence: 0,
        type: "text",
        text: "Message Type: FINAL_ANSWER\nTask name: main\nSender: worker\nPayload:\ndone",
        truncated: false,
        status: "completed",
        createdAt: timestamp,
        updatedAt: timestamp,
        completedAt: timestamp,
      }],
    });
    const mail = {
      requestId: "req_agent_mail",
      workspaceId: "wksp_agent_mail",
      sessionId: "sesn_agent_mail_dedup",
      sessionThreadId: "thrd_agent_mail_main",
      bindingId: "bind_agent_mail",
      bindingGeneration: 1,
      targetPodUid: "pod_agent_mail",
      runtimeInputId: "agent_mail:delivery_agent_mail",
      eventIds: ["sevt_agent_mail_received"],
      sequenceFrom: 2,
      sequenceTo: 2,
      kind: "inter_agent_message",
      deliveryId: "delivery_agent_mail",
      sourceThreadId: "thrd_agent_mail_child",
      sourceToolUseEventId: "sevt_agent_mail_spawn",
      message,
    } satisfies RuntimeAcceptedInputState;

    expect(state.enqueueAcceptedInput(mail)).toBe("applied");
    expect(state.enqueueAcceptedInput(mail)).toBe("duplicate");
    state.acknowledgeAcceptedInput(mail.runtimeInputId);
    expect(state.peekAcceptedInput()).toBeUndefined();
    expect(state.enqueueAcceptedInput(mail)).toBe("duplicate");
  });

  test("interrupt fence preserves queued stamped completion mail", () => {
    const state = new SessionState("sesn_agent_mail_interrupt_fence");
    const message = RuntimeMessageSchema.parse({
      id: "msg_agent_mail_interrupt_fence",
      sessionId: "sesn_agent_mail_interrupt_fence",
      role: "user",
      origin: "runtime",
      sequence: 1,
      status: "completed",
      createdAt: timestamp,
      updatedAt: timestamp,
      parts: [{
        id: "part_agent_mail_interrupt_fence",
        sessionId: "sesn_agent_mail_interrupt_fence",
        messageId: "msg_agent_mail_interrupt_fence",
        sequence: 0,
        type: "text",
        text: "completion",
        truncated: false,
        status: "completed",
        createdAt: timestamp,
        updatedAt: timestamp,
        completedAt: timestamp,
      }],
    });
    const mail = {
      requestId: "req_agent_mail_interrupt_fence",
      workspaceId: "wksp_agent_mail_interrupt_fence",
      sessionId: "sesn_agent_mail_interrupt_fence",
      sessionThreadId: "thrd_agent_mail_main",
      bindingId: "bind_agent_mail_interrupt_fence",
      bindingGeneration: 1,
      targetPodUid: "pod_agent_mail_interrupt_fence",
      runtimeInputId: "agent_mail:delivery_interrupt_fence",
      eventIds: ["sevt_agent_mail_interrupt_received"],
      sequenceFrom: 2,
      sequenceTo: 2,
      kind: "inter_agent_message",
      deliveryId: "delivery_interrupt_fence",
      sourceThreadId: "thrd_agent_mail_child",
      sourceToolUseEventId: "sevt_agent_mail_spawn",
      message,
    } satisfies RuntimeAcceptedInputState;

    expect(state.enqueueAcceptedInput(mail)).toBe("applied");
    state.discardQueuedAcceptedInputsBeforeFence(10);

    expect(state.peekAcceptedInput()).toEqual(mail);
    expect(state.enqueueAcceptedInput(mail)).toBe("duplicate");
  });

  test("task-notification identity deduplicates while the accepted fact remains queued", () => {
    const state = new SessionState("sesn_task_notification_queued");
    const notification = {
      requestId: "req_task_notification_queued",
      workspaceId: "wksp_task_notification_queued",
      sessionId: "sesn_task_notification_queued",
      sessionThreadId: "thrd_task_notification_queued",
      bindingId: "bind_task_notification_queued",
      bindingGeneration: 1,
      targetPodUid: "pod_task_notification_queued",
      runtimeInputId: "rin_task_notification_queued",
      eventIds: ["sevt_task_notification_queued"],
      sequenceFrom: 3,
      sequenceTo: 3,
      kind: "task_notification",
      taskId: "task_notification_queued",
      sourceToolUseEventId: "sevt_task_notification_source",
      status: "completed",
      payloadJson: "{\"status\":\"completed\"}",
      commit: async () => ({ ok: true, stale: true }),
    } satisfies RuntimeAcceptedInputState;

    expect(state.enqueueAcceptedInput(notification)).toBe("applied");
    expect(state.enqueueAcceptedInput(notification)).toBe("duplicate");
    expect(state.peekAcceptedInput()).toBe(notification);
  });

  test("task-notification identity deduplicates after its durable message is committed", () => {
    const state = new SessionState("sesn_task_notification_committed");
    const committedMessage = RuntimeMessageSchema.parse({
      id: "msg_task_notification_committed",
      sessionId: "sesn_task_notification_committed",
      role: "user",
      origin: "runtime",
      sequence: 4,
      status: "completed",
      createdAt: timestamp,
      updatedAt: timestamp,
      parts: [{
        id: "part_task_notification_committed",
        sessionId: "sesn_task_notification_committed",
        messageId: "msg_task_notification_committed",
        sequence: 0,
        type: "text",
        text: "Task completed.",
        truncated: false,
        status: "completed",
        createdAt: timestamp,
        updatedAt: timestamp,
        completedAt: timestamp,
      }],
    });
    const notification = {
      requestId: "req_task_notification_committed",
      workspaceId: "wksp_task_notification_committed",
      sessionId: "sesn_task_notification_committed",
      sessionThreadId: "thrd_task_notification_committed",
      bindingId: "bind_task_notification_committed",
      bindingGeneration: 1,
      targetPodUid: "pod_task_notification_committed",
      runtimeInputId: "rin_task_notification_committed",
      eventIds: ["sevt_task_notification_committed"],
      sequenceFrom: 4,
      sequenceTo: 4,
      taskId: "task_notification_committed",
      sourceToolUseEventId: "sevt_task_notification_source",
      status: "completed",
      payloadJson: "{\"status\":\"completed\"}",
      committedMessage,
    } satisfies RuntimeTaskNotificationState;

    expect(state.commitTaskNotification(notification)).toBe("applied");
    expect(state.commitTaskNotification(notification)).toBe("duplicate");
    expect(state.contextManager.messages()).toEqual([committedMessage]);
  });

  test("clear removes transient attachments and model-only messages", () => {
    const state = new SessionState("sesn_clear");
    state.addPendingAttachments([{
      transient: {
        attachmentRef: "att_1",
        sourceToolUseEventId: "sevt_tool_1",
        sourcePath: "/tmp/image.png",
        pageRange: "",
        detail: "auto",
      },
      fileBacked: undefined,
      mime: "image/png",
      filename: "image.png",
    }]);
    state.addTransientModelMessage(RuntimeMessageSchema.parse({
      id: "msg_model_only",
      sessionId: "sesn_clear",
      role: "user",
      origin: "runtime",
      sequence: 0,
      status: "completed",
      createdAt: timestamp,
      updatedAt: timestamp,
      parts: [{
        id: "part_model_only",
        sessionId: "sesn_clear",
        messageId: "msg_model_only",
        sequence: 0,
        type: "text",
        text: "transient",
        truncated: false,
        status: "completed",
        createdAt: timestamp,
        updatedAt: timestamp,
        completedAt: timestamp,
      }],
    }));

    state.clear();

    expect(state.pendingAttachments()).toEqual([]);
    expect(state.transientModelMessages()).toEqual([]);
    expect(state.transientModelMessageCount()).toBe(0);
  });
});
