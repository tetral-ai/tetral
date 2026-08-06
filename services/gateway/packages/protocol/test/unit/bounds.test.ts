import { describe, expect, test } from "bun:test";
import { readFile } from "node:fs/promises";
import {
  ProviderFinishReason,
  ProviderRequest as ProviderRequestMessage,
  ProviderRequestKind,
  ProviderStreamEvent as ProviderStreamEventMessage,
  ProviderStreamEventType,
  RuntimeMessageRole,
  RuntimeToolPartState,
  SystemCacheHint,
  SystemSegmentKind,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { ProviderStreamEvent } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import {
  MaxIdBytes,
  MaxMetadataBytes,
  MaxProviderRequestToolOutputJsonBytes,
  MaxProviderRequestAttachments,
  MaxProviderRequestMessagePartJsonBytes,
  MaxProviderToolCallInputJsonBytes,
  MaxProviderUsageJsonBytes,
  MaxTextBytes,
  truncateUtf8Bytes,
  validateProviderRequest,
  validateProviderStreamEvent,
} from "@tetral/gateway-protocol/src/bounds.js";
import { validProviderRequest } from "./fixtures.js";

const approvalReviewerOutputSchemaJson = await readFile(
  new URL("../../../../../agent-runtime/packages/runtime-pod/src/assets/approval-reviewer-output-schema.json", import.meta.url),
  "utf8",
);

describe("Gateway protocol bounds", () => {
  test("keeps outbound reasoning deltas at the 64 KiB text and 16 KiB metadata boundaries", () => {
    const request = validProviderRequest();
    const base = {
      requestId: request.requestId,
      modelRequestId: request.modelRequestId,
      sequence: 1,
      type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_DELTA,
    };
    const metadataAtLimit = `{"x":"${"m".repeat(MaxMetadataBytes - 8)}"}`;
    expect(validateProviderStreamEvent({
      ...base,
      reasoning: { id: "reasoning_1", text: "x".repeat(MaxTextBytes), metadataJson: metadataAtLimit },
    }, request)).toEqual({ ok: true });
    expect(validateProviderStreamEvent({
      ...base,
      reasoning: { id: "reasoning_1", text: "x".repeat(MaxTextBytes + 1), metadataJson: "{}" },
    }, request)).not.toEqual({ ok: true });
    expect(validateProviderStreamEvent({
      ...base,
      reasoning: { id: "reasoning_1", text: "x", metadataJson: `{"x":"${"m".repeat(MaxMetadataBytes - 7)}"}` },
    }, request)).not.toEqual({ ok: true });
  });

  test("accepts a valid Runtime-to-Gateway ProviderRequest snapshot", () => {
    expect(validateProviderRequest(validProviderRequest())).toEqual({ ok: true });
  });

  test("admits multi-megabyte message parts while keeping system segments at 64 KiB", () => {
    expect(MaxProviderRequestMessagePartJsonBytes).toBe(16 * 1024 * 1024);
    const multiMegabyteText = "x".repeat(2 * 1024 * 1024);
    const base = validProviderRequest();
    for (const part of [
      { id: "part_text", text: { text: multiMegabyteText } },
      { id: "part_reasoning", reasoning: { text: multiMegabyteText, metadataJson: "{}" } },
    ]) {
      expect(validateProviderRequest({
        ...base,
        messages: [{ ...base.messages[0]!, parts: [part] }],
      })).toEqual({ ok: true });
    }

    expectInvalid(validateProviderRequest({
      ...base,
      system: [{ ...base.system[0]!, text: "x".repeat(MaxTextBytes + 1) }],
    }));
  });

  test("rejects empty text parts while retaining signed empty reasoning", () => {
    const base = validProviderRequest();
    expectInvalid(validateProviderRequest({
      ...base,
      messages: [{
        ...base.messages[0]!,
        parts: [{ id: "part_empty_text", text: { text: "" } }],
      }],
    }));
    expect(validateProviderRequest({
      ...base,
      messages: [{
        ...base.messages[0]!,
        role: RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_ASSISTANT,
        parts: [{
          id: "part_signed_empty_reasoning",
          reasoning: { text: "", metadataJson: JSON.stringify({ anthropic: { signature: "sig_1" } }) },
        }],
      }],
    })).toEqual({ ok: true });
  });

  test("admits signed empty reasoning while retaining the reasoning byte ceiling", () => {
    const base = validProviderRequest();
    const withReasoning = (text: string) => ({
      ...base,
      messages: [{
        ...base.messages[0]!,
        role: RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_ASSISTANT,
        parts: [{
          id: "part_signed_reasoning",
          reasoning: { text, metadataJson: JSON.stringify({ anthropic: { signature: "sig_1" } }) },
        }],
      }],
    });

    expect(validateProviderRequest(withReasoning(""))).toEqual({ ok: true });
    expect(validateProviderRequest(withReasoning("x".repeat(MaxProviderRequestMessagePartJsonBytes - 2)))).toEqual({ ok: true });
    expectInvalid(validateProviderRequest(withReasoning("x".repeat(MaxProviderRequestMessagePartJsonBytes - 1))));
  });

  test("uses the protocol identifier limit for provider stream ids", () => {
    const base = validProviderRequest();
    const eventBase = {
      requestId: base.requestId,
      modelRequestId: base.modelRequestId,
      sequence: 1,
      type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START,
    };
    expect(validateProviderStreamEvent({
      ...eventBase,
      text: { id: "i".repeat(MaxIdBytes), text: "", metadataJson: "{}" },
    }, base)).toEqual({ ok: true });
    expect(validateProviderStreamEvent({
      ...eventBase,
      text: { id: "i".repeat(MaxIdBytes + 1), text: "", metadataJson: "{}" },
    }, base)).not.toEqual({ ok: true });
  });

  test("accepts all contract-owned ProviderRequest request kinds", () => {
    for (const requestKind of [
      ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
      ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY,
      ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
      ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER_COMPACTION,
    ]) {
      expect(validateProviderRequest(validProviderRequest({
        requestKind,
        outputSchemaJson: requestKind === ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER
          ? approvalReviewerOutputSchemaJson
          : undefined,
      }))).toEqual({ ok: true });
    }
  });

  test("requires a request-level output schema only for approval reviewer requests", () => {
    expect(validateProviderRequest(validProviderRequest({
      requestKind: ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
      outputSchemaJson: approvalReviewerOutputSchemaJson,
    }))).toEqual({ ok: true });

    for (const requestKind of [
      ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
      ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY,
      ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER_COMPACTION,
    ]) {
      expect(validateProviderRequest(validProviderRequest({ requestKind, outputSchemaJson: undefined }))).toEqual({ ok: true });
      expectInvalid(validateProviderRequest(validProviderRequest({ requestKind, outputSchemaJson: approvalReviewerOutputSchemaJson })));
    }

    for (const outputSchemaJson of [undefined, "", "not-json", "[]", JSON.stringify(true)]) {
      expectInvalid(validateProviderRequest(validProviderRequest({
        requestKind: ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
        outputSchemaJson,
      })));
    }
  });

  test("round-trips the request-level output schema through generated protobuf stubs", () => {
    const request = validProviderRequest({
      requestKind: ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
      outputSchemaJson: approvalReviewerOutputSchemaJson,
    });

    const decoded = ProviderRequestMessage.decode(ProviderRequestMessage.encode(request).finish());

    expect(decoded.outputSchemaJson).toBe(approvalReviewerOutputSchemaJson);
    expect(validateProviderRequest(decoded)).toEqual({ ok: true });
  });

  test("accepts all contract-owned system segment and runtime tool states", () => {
    const base = validProviderRequest();
    const system = base.system[0]!;
    for (const kind of [
      SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE,
      SystemSegmentKind.SYSTEM_SEGMENT_KIND_TOOL_GUIDANCE,
      SystemSegmentKind.SYSTEM_SEGMENT_KIND_AGENT,
      SystemSegmentKind.SYSTEM_SEGMENT_KIND_MEMORY,
      SystemSegmentKind.SYSTEM_SEGMENT_KIND_SKILL,
      SystemSegmentKind.SYSTEM_SEGMENT_KIND_APPROVAL_REVIEWER_POLICY,
    ]) {
      expect(validateProviderRequest(validProviderRequest({
        system: [{ ...system, kind }],
      }))).toEqual({ ok: true });
    }
    for (const cacheHint of [
      SystemCacheHint.SYSTEM_CACHE_HINT_STABLE,
      SystemCacheHint.SYSTEM_CACHE_HINT_SESSION,
      SystemCacheHint.SYSTEM_CACHE_HINT_NONE,
    ]) {
      expect(validateProviderRequest(validProviderRequest({
        system: [{ ...system, cacheHint }],
      }))).toEqual({ ok: true });
    }
    for (const state of [
      RuntimeToolPartState.RUNTIME_TOOL_PART_STATE_COMPLETED,
      RuntimeToolPartState.RUNTIME_TOOL_PART_STATE_ERROR,
      RuntimeToolPartState.RUNTIME_TOOL_PART_STATE_CANCELLED,
    ]) {
      expect(validateProviderRequest(validProviderRequest({
        messages: [{
          ...base.messages[0]!,
          parts: [{
            id: "part_tool",
            tool: {
              callId: "call_1",
              name: "Read",
              toolUseEventId: "sevt_tool_1",
              state,
              inputJson: "{}",
              outputOrErrorJson: "{}",
            },
          }],
        }],
      }))).toEqual({ ok: true });
    }
  });

  test("uses independent byte ceilings for provider tool input, tool output, and usage JSON", () => {
    const base = validProviderRequest();
    const message = base.messages[0]!;
    const toolPart = {
      id: "part_tool",
      tool: {
        callId: "call_1",
        name: "Read",
        toolUseEventId: "sevt_tool_1",
        state: RuntimeToolPartState.RUNTIME_TOOL_PART_STATE_COMPLETED,
        inputJson: jsonObjectAtBytes(MaxProviderToolCallInputJsonBytes),
        outputOrErrorJson: jsonObjectAtBytes(MaxProviderRequestToolOutputJsonBytes),
      },
    };
    expect(validateProviderRequest({
      ...base,
      messages: [{ ...message, parts: [toolPart] }],
    })).toEqual({ ok: true });
    expectInvalid(validateProviderRequest({
      ...base,
      messages: [{
        ...message,
        parts: [{
          ...toolPart,
          tool: { ...toolPart.tool, outputOrErrorJson: jsonObjectAtBytes(MaxProviderRequestToolOutputJsonBytes + 1) },
        }],
      }],
    }));

    const eventBase = {
      requestId: base.requestId,
      modelRequestId: base.modelRequestId,
      type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_CALL,
    };
    expect(validateProviderStreamEvent({
      ...eventBase,
      toolCall: { id: "call_large", name: "memory", inputJson: jsonObjectAtBytes(MaxProviderToolCallInputJsonBytes), metadataJson: "{}" },
    }, base)).toEqual({ ok: true });
    expectInvalid(validateProviderStreamEvent({
      ...eventBase,
      toolCall: { id: "call_large", name: "memory", inputJson: jsonObjectAtBytes(MaxProviderToolCallInputJsonBytes + 1), metadataJson: "{}" },
    }, base));

    const finish = (providerUsageJson: string): ProviderStreamEvent => ({
      requestId: base.requestId,
      modelRequestId: base.modelRequestId,
      type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
      finish: {
        reason: ProviderFinishReason.PROVIDER_FINISH_REASON_STOP,
        usage: {
          inputTotalTokens: 1,
          inputUncachedTokens: 1,
          outputTotalTokens: 1,
          providerUsageJson,
        },
        metadataJson: "{}",
      },
    });
    expect(validateProviderStreamEvent(finish(jsonObjectAtBytes(MaxProviderUsageJsonBytes)), base)).toEqual({ ok: true });
    expectInvalid(validateProviderStreamEvent(finish(jsonObjectAtBytes(MaxProviderUsageJsonBytes + 1)), base));
  });

  test("truncates provider errors by UTF-8 bytes without splitting code points", () => {
    expect(truncateUtf8Bytes(`${"a".repeat(5)}éé`, 8)).toBe("aaaaaé");
    expect(new TextEncoder().encode(truncateUtf8Bytes("你".repeat(4), 8)).byteLength).toBe(6);
  });

  test("allows an absent tool use event id only for internal error repairs", () => {
    const base = validProviderRequest();
    const message = base.messages[0]!;
    const repairPart = {
      id: "internal_invalid_tool:mreq_1:call_repair:unknown_tool:part",
      tool: {
        callId: "call_repair",
        name: "unknown_tool",
        toolUseEventId: undefined,
        state: RuntimeToolPartState.RUNTIME_TOOL_PART_STATE_ERROR,
        inputJson: "{}",
        outputOrErrorJson: "{}",
      },
    };

    expect(validateProviderRequest(validProviderRequest({
      messages: [{ ...message, parts: [repairPart] }],
    }))).toEqual({ ok: true });
    expect(validateProviderRequest(validProviderRequest({
      messages: [{
        ...message,
        parts: [{
          ...repairPart,
          tool: { ...repairPart.tool, state: RuntimeToolPartState.RUNTIME_TOOL_PART_STATE_COMPLETED },
        }],
      }],
    }))).toEqual({
      ok: false,
      message: "invalid internal request",
      code: "invalid_message_part",
      member: "messages.parts",
    });
  });

  test("validates ProviderRequest attachment lowering hints", () => {
    const base = validProviderRequest();
    const attachment = base.attachments[0]!;
    for (const mime of ["application/pdf", "image/png", "image/jpeg", "image/gif", "image/webp"]) {
      expect(validateProviderRequest(validProviderRequest({
        attachments: [{ ...attachment, mime }],
      }))).toEqual({ ok: true });
    }
    expect(validateProviderRequest(validProviderRequest({
      attachments: [{ ...attachment, transient: { ...attachment.transient!, pageRange: "1-3,5", detail: "high" } }],
    }))).toEqual({ ok: true });
    expect(validateProviderRequest(validProviderRequest({
      attachments: [{ ...attachment, transient: { ...attachment.transient!, pageRange: "", detail: "auto" } }],
    }))).toEqual({ ok: true });
    expect(validateProviderRequest(validProviderRequest({
      attachments: [{ ...attachment, transient: { ...attachment.transient!, pageRange: "", detail: "provider-defined-detail" } }],
    }))).toEqual({ ok: true });
    expect(validateProviderRequest(validProviderRequest({
      attachments: [{ ...attachment, transient: { ...attachment.transient!, pageRange: "", detail: "d".repeat(128) } }],
    }))).toEqual({ ok: true });
    expect(validateProviderRequest(validProviderRequest({
      attachments: [{
        transient: undefined,
        fileBacked: { sourceEventId: "sevt_user_1", fileId: "file_1" },
        mime: "text/plain",
        filename: "notes.txt",
      }],
    }))).toEqual({ ok: true });

    for (const request of [
      validProviderRequest({ attachments: [{ ...attachment, transient: { ...attachment.transient!, pageRange: "0", detail: "auto" } }] }),
      validProviderRequest({ attachments: [{ ...attachment, transient: { ...attachment.transient!, pageRange: "3-1", detail: "auto" } }] }),
      validProviderRequest({ attachments: [{ ...attachment, transient: { ...attachment.transient!, pageRange: "1, two", detail: "auto" } }] }),
      validProviderRequest({ attachments: [{ ...attachment, transient: { ...attachment.transient!, pageRange: "1-2,".repeat(32), detail: "auto" } }] }),
      validProviderRequest({ attachments: [{ ...attachment, transient: { ...attachment.transient!, pageRange: "", detail: "" } }] }),
      validProviderRequest({ attachments: [{ ...attachment, transient: { ...attachment.transient!, pageRange: "", detail: "d".repeat(129) } }] }),
      validProviderRequest({ attachments: [{ ...attachment, mime: "image/svg+xml" }] }),
      validProviderRequest({ attachments: [{ ...attachment, mime: "image/bmp" }] }),
      validProviderRequest({ attachments: [{ ...attachment, mime: "text/plain" }] }),
      validProviderRequest({ attachments: [{ ...attachment, transient: undefined, fileBacked: undefined }] }),
      validProviderRequest({
        attachments: [{
          ...attachment,
          fileBacked: { sourceEventId: "sevt_user_1", fileId: "file_1" },
        }],
      }),
    ]) {
      expectInvalid(validateProviderRequest(request));
    }
  });

  test("round-trips exactly one ProviderRequest attachment origin", () => {
    const transient = validProviderRequest().attachments[0]!;
    const fileBacked = {
      transient: undefined,
      fileBacked: { sourceEventId: "sevt_user_1", fileId: "file_1" },
      mime: "application/pdf",
      filename: "report.pdf",
    };

    for (const attachment of [transient, fileBacked]) {
      const request = validProviderRequest({ attachments: [attachment] });
      const decoded = ProviderRequestMessage.decode(ProviderRequestMessage.encode(request).finish());
      expect(decoded.attachments).toEqual([attachment]);
      expect(validateProviderRequest(decoded)).toEqual({ ok: true });
    }
  });

  test("rejects ProviderRequest attachment overflow before provider work", () => {
    const attachment = validProviderRequest().attachments[0]!;
    const attachments = Array.from({ length: MaxProviderRequestAttachments }, (_, index) => ({
      ...attachment,
      transient: { ...attachment.transient!, attachmentRef: `att_${index}` },
    }));

    expect(validateProviderRequest(validProviderRequest({ attachments }))).toEqual({ ok: true });
    expectInvalid(validateProviderRequest(validProviderRequest({
      attachments: [...attachments, { ...attachment, transient: { ...attachment.transient!, attachmentRef: "att_overflow" } }],
    })));
  });

  test("rejects duplicate file-backed attachment origins before provider work", () => {
    const fileBacked = {
      transient: undefined,
      fileBacked: { sourceEventId: "sevt_user_duplicate", fileId: "file_duplicate" },
      mime: "image/png",
      filename: "duplicate.png",
    };

    expectInvalid(validateProviderRequest(validProviderRequest({
      attachments: [fileBacked, { ...fileBacked, filename: "same-origin-again.png" }],
    })));
  });

  test("rejects malformed ProviderRequest fields before Gateway side effects", () => {
    const base = validProviderRequest();
    for (const request of [
      { ...base, requestId: "" },
      { ...base, modelRequestId: "" },
      { ...base, workspaceId: "" },
      { ...base, sessionId: "" },
      { ...base, sessionThreadId: "" },
      { ...base, bindingId: "" },
      { ...base, bindingGeneration: 0 },
      { ...base, runtimeBindingToken: "" },
      { ...base, requestKind: 0 },
      { ...base, model: undefined },
      { ...base, model: { providerId: "", modelId: "gpt-test", variant: "" } },
      { ...base, tools: [{ ...base.tools[0]!, inputSchemaJson: "[]" }] },
      { ...base, tools: [{ ...base.tools[0]!, inputSchemaJson: "not-json" }] },
      { ...base, attachments: [{ ...base.attachments[0]!, mime: "text/plain" }] },
      { ...base, limits: undefined },
      { ...base, limits: { maxOutputTokens: -1, timeoutMs: 30_000 } },
      { ...base, messages: [{ ...base.messages[0]!, role: 99 as RuntimeMessageRole }] },
      { ...base, messages: [{ ...base.messages[0]!, status: "streaming" }] },
      { ...base, messages: [{ ...base.messages[0]!, status: "pending" }] },
      { ...base, messages: [{ ...base.messages[0]!, origin: "system" }] },
    ]) {
      expectInvalid(validateProviderRequest(request));
    }
  });

  test("accepts a zero output limit as unset so the model's documented limit governs", () => {
    const base = validProviderRequest();
    expect(validateProviderRequest({
      ...base,
      limits: { maxOutputTokens: 0, timeoutMs: 30_000 },
    })).toEqual({ ok: true });
  });

  test("validates ProviderStreamEvent request identity and payload shape", () => {
    const request = validProviderRequest();
    expect(validateProviderStreamEvent({
      requestId: request.requestId,
      modelRequestId: request.modelRequestId,
      type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR,
      providerError: {
        metadataJson: "{}",
        error: {
          code: "provider_unavailable",
          message: "unavailable",
          retryable: true,
          fatal: false,
          statusCode: 503,
          retryAfterMs: 0,
        },
      },
    }, request)).toEqual({ ok: true });
    expectInvalid(validateProviderStreamEvent({
      requestId: "wrong",
      modelRequestId: request.modelRequestId,
      type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR,
      providerError: {
        metadataJson: "{}",
        error: {
          code: "provider_unavailable",
          message: "unavailable",
          retryable: true,
          fatal: false,
          statusCode: 503,
          retryAfterMs: 0,
        },
      },
    }, request));
  });

  test("accepts one bounded non-terminal attachment rejection envelope", () => {
    const request = validProviderRequest();
    const event = {
      requestId: request.requestId,
      modelRequestId: request.modelRequestId,
      type: 13,
      attachmentRejections: {
        rejections: [
          {
            transient: {
              attachmentRef: "att_expired",
              sourceToolUseEventId: "sevt_tool_expired",
              sourcePath: "mcp:test/expired.png",
              pageRange: "",
              detail: "auto",
            },
            fileBacked: undefined,
            reason: 1,
          },
          {
            transient: undefined,
            fileBacked: {
              sourceEventId: "sevt_user_large",
              fileId: "file_large",
            },
            reason: 2,
          },
        ],
      },
    } as unknown as ProviderStreamEvent;

    expect(validateProviderStreamEvent(event, request)).toEqual({ ok: true });
  });

  test("rejects negative ProviderError numeric fields", () => {
    const request = validProviderRequest();
    const providerError = {
      metadataJson: "{}",
      error: {
        code: "provider_unavailable",
        message: "unavailable",
        retryable: true,
        fatal: false,
        statusCode: 503,
        retryAfterMs: 0,
      },
    };

    for (const error of [
      { ...providerError.error, statusCode: -1 },
      { ...providerError.error, retryAfterMs: -1 },
    ]) {
      expectInvalid(validateProviderStreamEvent({
        requestId: request.requestId,
        modelRequestId: request.modelRequestId,
        type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR,
        providerError: { ...providerError, error },
      }, request));
    }
  });

  test("rejects malformed ProviderStreamEvent payloads per event variant", () => {
    const request = validProviderRequest();
    const base = {
      requestId: request.requestId,
      modelRequestId: request.modelRequestId,
    };
    const finishUsage = {
      inputTotalTokens: 1,
      inputUncachedTokens: 1,
      outputTotalTokens: 2,
      totalTokens: 3,
      providerUsageJson: "{}",
    };

    expect(validateProviderStreamEvent({
      ...base,
      type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
      finish: {
        reason: ProviderFinishReason.PROVIDER_FINISH_REASON_STOP,
        usage: finishUsage,
        metadataJson: "{}",
        contextWindowTokens: 500_000,
        inputLimitTokens: 372_000,
        outputTokenLimit: 128_000,
      },
    }, request)).toEqual({ ok: true });
    const finishRoundTrip = ProviderStreamEventMessage.decode(ProviderStreamEventMessage.encode({
      ...base,
      type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
      finish: {
        reason: ProviderFinishReason.PROVIDER_FINISH_REASON_STOP,
        usage: finishUsage,
        metadataJson: "{}",
        contextWindowTokens: 500_000,
        inputLimitTokens: 372_000,
        outputTokenLimit: 128_000,
      },
    }).finish());
    expect(finishRoundTrip.finish).toMatchObject({
      contextWindowTokens: 500_000,
      inputLimitTokens: 372_000,
      outputTokenLimit: 128_000,
    });
    for (const reason of [
      ProviderFinishReason.PROVIDER_FINISH_REASON_STOP,
      ProviderFinishReason.PROVIDER_FINISH_REASON_LENGTH,
      ProviderFinishReason.PROVIDER_FINISH_REASON_TOOL_CALLS,
      ProviderFinishReason.PROVIDER_FINISH_REASON_CONTENT_FILTER,
      ProviderFinishReason.PROVIDER_FINISH_REASON_ERROR,
      ProviderFinishReason.PROVIDER_FINISH_REASON_OTHER,
      ProviderFinishReason.PROVIDER_FINISH_REASON_UNKNOWN,
    ]) {
      expect(validateProviderStreamEvent({
        ...base,
        type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
        finish: {
          reason,
          usage: finishUsage,
          metadataJson: "{}",
        },
      }, request)).toEqual({ ok: true });
    }

    for (const malformed of [
      {
        ...base,
        modelRequestId: "wrong",
        type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_DELTA,
        text: { id: "text_1", text: "hello", metadataJson: "{}" },
      },
      {
        ...base,
        type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_UNSPECIFIED,
        text: { id: "text_1", text: "hello", metadataJson: "{}" },
      },
      {
        ...base,
        type: ProviderStreamEventType.UNRECOGNIZED,
        text: { id: "text_1", text: "hello", metadataJson: "{}" },
      },
      {
        ...base,
        type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_DELTA,
      },
      {
        ...base,
        type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_DELTA,
        reasoning: { id: "rsn_1", text: "wrong-payload", metadataJson: "{}" },
      },
      {
        ...base,
        type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_DELTA,
        text: { id: "text_1", text: "hello", metadataJson: "{}" },
        reasoning: { id: "rsn_1", text: "extra-payload", metadataJson: "{}" },
      },
      {
        ...base,
        type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START,
        text: { id: "text_1", text: "not-a-delta", metadataJson: "{}" },
      },
      {
        ...base,
        type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_END,
        reasoning: { id: "rsn_1", text: "not-a-delta", metadataJson: "{}" },
      },
      {
        ...base,
        type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_START,
        toolInput: { id: "tool_1", name: "Read", text: "{\"path\":\"/tmp/a\"}", metadataJson: "{}" },
      },
      {
        ...base,
        type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_DELTA,
        text: { id: "text_1", text: "hello", metadataJson: "not-json" },
      },
      {
        ...base,
        type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
        finish: {
          reason: ProviderFinishReason.PROVIDER_FINISH_REASON_UNSPECIFIED,
          usage: finishUsage,
          metadataJson: "{}",
        },
      },
      {
        ...base,
        type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
        finish: {
          reason: ProviderFinishReason.PROVIDER_FINISH_REASON_STOP,
          usage: { ...finishUsage, inputTotalTokens: -1 },
          metadataJson: "{}",
        },
      },
      {
        ...base,
        type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
        finish: {
          reason: ProviderFinishReason.PROVIDER_FINISH_REASON_STOP,
          usage: { ...finishUsage, providerUsageJson: "not-json" },
          metadataJson: "{}",
        },
      },
      {
        ...base,
        type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
        finish: {
          reason: ProviderFinishReason.PROVIDER_FINISH_REASON_STOP,
          usage: finishUsage,
          metadataJson: "{}",
          contextWindowTokens: 0,
        },
      },
      {
        ...base,
        type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
        finish: {
          reason: ProviderFinishReason.PROVIDER_FINISH_REASON_STOP,
          usage: finishUsage,
          metadataJson: "{}",
          inputLimitTokens: 1.5,
        },
      },
      {
        ...base,
        type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
        finish: {
          reason: ProviderFinishReason.PROVIDER_FINISH_REASON_STOP,
          usage: finishUsage,
          metadataJson: "{}",
          outputTokenLimit: -1,
        },
      },
      {
        ...base,
        type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR,
        providerError: { error: undefined, metadataJson: "{}" },
      },
    ] satisfies readonly ProviderStreamEvent[]) {
      expectInvalid(validateProviderStreamEvent(malformed, request));
    }
  });
});

function expectInvalid(result: { readonly ok: boolean; readonly message?: string }) {
  expect(result.ok).toBe(false);
  if (!result.ok) {
    expect(result.message).toBe("invalid internal request");
  }
}

function jsonObjectAtBytes(bytes: number): string {
  const fixedBytes = new TextEncoder().encode('{"x":""}').byteLength;
  return `{"x":"${"x".repeat(bytes - fixedBytes)}"}`;
}
