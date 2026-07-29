import { describe, expect, test } from "bun:test";
import {
  acceptedInputDrafts,
  applyAcceptedInputReceipt,
} from "../../src/runtime/runtime-declaration.js";
import {
  DurableRuntimeMessageSchema,
  RuntimeMessageDraftSchema,
  RuntimePartDraftSchema,
} from "../../src/contracts/runtime.js";
import { stableRuntimeID } from "../../src/runtime/runtime-identity.js";
import { acceptedInputReceipt } from "./runtime-declaration-fixtures.js";
import type { RuntimeAcceptedInputState } from "../../src/session/session-state.js";

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
});
