import { describe, expect, test } from "bun:test";
import { RuntimeMessageSchema } from "../../src/contracts/runtime.js";
import type { RuntimeAcceptedInputState } from "../../src/session/session-state.js";
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

  test("cold family carrier survives later hot runtime config patches", () => {
    const state = new SessionState("sesn_cold_family");
    const scope = {
      requestId: "req_config",
      workspaceId: "default",
      sessionId: "sesn_cold_family",
      sessionThreadId: "thrd_cold_family",
      bindingId: "bind_cold_family",
      bindingGeneration: 1,
      targetPodUid: "pod_cold_family",
      eventIds: [] as const,
      sequenceFrom: 0,
      sequenceTo: 0,
    };
    const cold = {
      ...scope,
      runtimeInputId: "rin_cold",
      generation: 1,
      payloadJson: JSON.stringify({ runtime_config: { installedTools: [{ type: "tetral_agent_toolset", family: "claude" }] } }),
      coldLoad: true as const,
      installedBuiltinFamily: "claude" as const,
    };
    const hot = {
      ...scope,
      runtimeInputId: "rin_hot",
      generation: 2,
      payloadJson: JSON.stringify({ tool_policy: { approvalMode: "full_access" } }),
    };
    expect(state.applyRuntimeConfigPatch(cold)).toBe("applied");
    expect(state.applyRuntimeConfigPatch(hot)).toBe("applied");
    expect(state.runtimeConfigPatches()).toEqual([cold, hot]);
    expect(state.installedBuiltinFamily()).toBe("claude");
  });

  test("MCP manifest apply is generation-monotonic across etag flaps and stale late delivery", () => {
    const state = new SessionState("sesn_mcp_generation");
    const scope = {
      requestId: "req_mcp_generation",
      workspaceId: "default",
      sessionId: "sesn_mcp_generation",
      sessionThreadId: "thrd_mcp_generation",
      bindingId: "bind_mcp_generation",
      bindingGeneration: 4,
      targetPodUid: "pod_mcp_generation",
      eventIds: [] as const,
      sequenceFrom: 0,
      sequenceTo: 0,
    };
    const generationOne = {
      ...scope,
      runtimeInputId: "rin_mcp_1",
      generation: 1,
      mcpServerName: "github",
      manifestETag: "etag_a",
      payloadJson: "{\"mcp_manifest\":{\"mcp_server_name\":\"github\",\"manifest_etag\":\"etag_a\",\"manifest_generation\":1,\"tools\":[]}}",
    };
    const generationThree = {
      ...scope,
      runtimeInputId: "rin_mcp_3",
      generation: 3,
      mcpServerName: "github",
      manifestETag: "etag_a",
      payloadJson: "{\"mcp_manifest\":{\"mcp_server_name\":\"github\",\"manifest_etag\":\"etag_a\",\"manifest_generation\":3,\"tools\":[]}}",
    };
    const staleGenerationTwo = {
      ...scope,
      runtimeInputId: "rin_mcp_2_late",
      generation: 2,
      mcpServerName: "github",
      manifestETag: "etag_b",
      payloadJson: "{\"mcp_manifest\":{\"mcp_server_name\":\"github\",\"manifest_etag\":\"etag_b\",\"manifest_generation\":2,\"tools\":[]}}",
    };
    const generationFourUnready = {
      ...scope,
      runtimeInputId: "rin_mcp_4",
      generation: 4,
      mcpServerName: "github",
      manifestReadiness: "unready" as const,
      manifestDiagnostic: "delivery_exhausted",
      payloadJson: "{\"mcp_manifest\":{\"mcp_server_name\":\"github\",\"manifest_generation\":4,\"readiness\":\"unready\",\"diagnostic\":\"delivery_exhausted\"}}",
    };

    expect(state.applyRuntimeConfigPatch(generationOne)).toBe("applied");
    expect(state.applyRuntimeConfigPatch(generationOne)).toBe("stale");
    expect(state.applyRuntimeConfigPatch(generationThree)).toBe("applied");
    expect(state.applyRuntimeConfigPatch(staleGenerationTwo)).toBe("stale");
    expect(state.applyRuntimeConfigPatch(generationFourUnready)).toBe("applied");
    expect(state.runtimeConfigPatches()).toEqual([generationFourUnready]);
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

  test("agent-mail delivery identity deduplicates hot enqueue and cold rescan after ACK", () => {
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
      eventIds: [],
      sequenceFrom: 0,
      sequenceTo: 0,
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

  test("interrupt fence preserves queued completion mail regardless of its synthetic sequence", () => {
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
      eventIds: [],
      sequenceFrom: 0,
      sequenceTo: 0,
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
