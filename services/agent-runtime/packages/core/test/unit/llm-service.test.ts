import { describe, expect, test } from "bun:test";
import { Effect, Stream } from "effect";
import {
  ProviderAttachmentRejectionReason,
  ProviderFinishReason,
  ProviderRequestKind,
  ProviderThreadRole,
  ProviderThreadVisibility,
  ProviderStreamEventType,
  ProviderContextRole,
  SystemCacheHint,
  SystemSegmentKind,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type {
  ProviderRequest,
  ProviderStreamEvent,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { GatewayClient, GatewayClientError, LLMServiceError } from "../../src/llm/llm-service.js";
import type { LLMEvent } from "../../src/llm/llm-event.js";
import { createLLMService, streamLLMEvents } from "../../src/llm/llm-service.js";

function request(): ProviderRequest {
  return {
    requestId: "provider-request-1",
    outputContractVersion: 2,
    modelRequestStartEventId: "evt_00000000000000000000000000000003",
    threadRole: ProviderThreadRole.PROVIDER_THREAD_ROLE_MAIN,
    threadVisibility: ProviderThreadVisibility.PROVIDER_THREAD_VISIBILITY_PUBLIC,
    modelRequestId: "model-request-1",
    requestKind: ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
    workspaceId: "workspace-1",
    sessionId: "session-1",
    sessionThreadId: "thread-1",
    bindingId: "binding-1",
    bindingGeneration: 7,
    runtimeProcessId: "process-test",
    runtimeBindingToken: "runtime-binding-token-1",
    model: { providerId: "openai", modelId: "gpt-5.5", variant: "" },
    system: [
      {
        kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE,
        text: "base system",
        cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_STABLE,
      },
    ],
    context: [
      {
        role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
        content: [{ text: { text: "hello" } }],
      },
    ],
    tools: [],
    attachments: [],
    limits: { maxOutputTokens: 128, timeoutMs: 1000 },
  };
}

function gatewayClient(events: readonly ProviderStreamEvent[]): GatewayClient {
  return {
    async streamProviderRequest() {
      return {
        events: Stream.fromIterable(events.map((frame, index) => ({ ...frame, frameSequence: index + 1 }))),
        completion: Promise.resolve({ outcome: "eof" as const }),
        cancel: () => undefined,
      };
    },
  };
}

function event(type: ProviderStreamEventType, payload: Partial<Omit<ProviderStreamEvent, "type">> = {}): ProviderStreamEvent {
  return {
    type,
    frameSequence: 1,
    ...payload,
  };
}

const textEventId = "evt_fa63d49e16196e6ec78bca9f3402d409";
const thinkingEventId = "evt_1883d72b86beda7ed3672f8cab82ee9a";

function completeText(providerPartId = "text-1", text = "hello", eventId = textEventId): ProviderStreamEvent {
  return event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_COMPLETE, {
    textComplete: { providerPartId, eventId, text },
  });
}

function thinkingStarted(providerPartId = "reasoning-1", eventId = thinkingEventId): ProviderStreamEvent {
  return event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_THINKING_STARTED, {
    thinkingStarted: { providerPartId, eventId },
  });
}

function completeReasoning(providerPartId = "reasoning-1", eventId = thinkingEventId, providerMetadataJson = "{}"): ProviderStreamEvent {
  return event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_COMPLETE, {
    reasoningComplete: { providerPartId, thinkingEventId: eventId, text: "thinking", providerMetadataJson },
  });
}

function completeTool(modelToolCallId = "call-1", name = "lookup", inputJson = '{"q":"hi"}'): ProviderStreamEvent {
  return event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_CALL_COMPLETE, {
    toolCallComplete: { modelToolCallId, name, inputJson, providerMetadataJson: "{}" },
  });
}

function successfulFinish(reason: ProviderFinishReason): ProviderStreamEvent {
  return event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH, {
    finish: {
      reason,
      usage: {
        inputTotalTokens: 0,
        inputUncachedTokens: 0,
        outputTotalTokens: 0,
        totalTokens: 0,
        providerUsageJson: "{}",
      },
      metadataJson: "{}",
      contextWindowTokens: 500_000,
      outputTokenLimit: 128_000,
    },
  });
}

const emptyFinishUsage = {
  inputTokens: 0,
  outputTokens: 0,
  reasoningTokens: 0,
  cacheReadTokens: 0,
  cacheWriteTokens: 0,
  totalTokens: 0,
  providerUsageJson: "{}",
} as const;

const defaultFinishLimits = {
  contextWindowTokens: 500_000,
  outputTokenLimit: 128_000,
} as const;

async function collect(stream: Stream.Stream<LLMEvent, LLMServiceError>): Promise<readonly LLMEvent[]> {
  return await Effect.runPromise(Stream.runCollect(stream));
}

describe("LLMService Gateway boundary", () => {
  test("normalizes remote transport failures and local cancellation separately", async () => {
    for (const testCase of [
      { code: "gateway_unavailable" as const, aborted: false, wantReason: undefined },
      { code: "gateway_cancelled" as const, aborted: false, wantReason: undefined },
      { code: "gateway_cancelled" as const, aborted: true, wantReason: "runtime_shutdown" },
    ]) {
      const controller = new AbortController();
      if (testCase.aborted) {
        controller.abort();
      }
      const failure: GatewayClientError = {
        type: "gateway-client",
        code: testCase.code,
        message: "Gateway transport failed.",
        retryable: testCase.code === "gateway_unavailable",
        fatal: false,
      };
      const client: GatewayClient = {
        async streamProviderRequest() {
          throw failure;
        },
      };

      const error = await Effect.runPromise(
        Stream.runCollect(streamLLMEvents(request(), client, { abortSignal: controller.signal })).pipe(Effect.flip),
      );
      expect(error.error).toMatchObject({
        code: "gateway_stream_error",
        ...(testCase.wantReason !== undefined ? { reason: testCase.wantReason } : {}),
      });
      if (testCase.wantReason === undefined) {
        expect(error.error).not.toHaveProperty("reason");
      }
    }
  });

  test("preserves deterministic Gateway request rejection as a protocol failure", async () => {
    const client: GatewayClient = {
      async streamProviderRequest() {
        throw {
          type: "gateway-client",
          code: "gateway_protocol_error",
          message: "Gateway rejected the provider request.",
          retryable: false,
          fatal: true,
        };
      },
    };

    const error = await Effect.runPromise(
      Stream.runCollect(streamLLMEvents(request(), client)).pipe(Effect.flip),
    );

    expect(error.error).toMatchObject({
      code: "gateway_protocol_error",
      retryable: false,
      fatal: true,
    });
  });

  test("maps generated Gateway ProviderStreamEvent variants to Runtime LLMEvent variants", async () => {
    const service = createLLMService(gatewayClient([
      completeText(),
      thinkingStarted(),
      completeReasoning("reasoning-1", thinkingEventId, '{"anthropic":{"signature":"sig_1"}}'),
      event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH, {
        finish: {
          reason: ProviderFinishReason.PROVIDER_FINISH_REASON_STOP,
          usage: {
            inputTotalTokens: 5,
            inputUncachedTokens: 2,
            inputCacheReadTokens: 1,
            inputCacheWriteTokens: 2,
            outputTotalTokens: 3,
            outputReasoningTokens: 1,
            totalTokens: 8,
            providerUsageJson: "{\"provider\":\"openai\"}",
          },
          metadataJson: "{\"openai\":{\"responseId\":\"resp_1\"}}",
          contextWindowTokens: 500_000,
          inputLimitTokens: 372_000,
          outputTokenLimit: 128_000,
        },
      }),
    ]));

    expect(await collect(service.stream(request()))).toEqual([
      { type: "text-complete", providerPartId: "text-1", eventId: textEventId, text: "hello" },
      { type: "thinking-started", providerPartId: "reasoning-1", eventId: thinkingEventId },
      { type: "reasoning-complete", providerPartId: "reasoning-1", thinkingEventId, text: "thinking", providerMetadata: { anthropic: { signature: "sig_1" } } },
      {
        type: "finish",
        finishReason: "stop",
        usage: {
          inputTokens: 2,
          outputTokens: 3,
          reasoningTokens: 1,
          cacheReadTokens: 1,
          cacheWriteTokens: 2,
          totalTokens: 8,
          providerUsageJson: "{\"provider\":\"openai\"}",
        },
        providerMetadata: { openai: { responseId: "resp_1" } },
        modelLimits: {
          contextWindowTokens: 500_000,
          inputLimitTokens: 372_000,
          outputTokenLimit: 128_000,
        },
      },
    ]);
  });

  test("rejects successful finishes missing usage or route-effective limits", async () => {
    const completeFinish = {
      reason: ProviderFinishReason.PROVIDER_FINISH_REASON_STOP,
      usage: {
        inputTotalTokens: 5,
        inputUncachedTokens: 5,
        outputTotalTokens: 3,
        totalTokens: 8,
        providerUsageJson: "{}",
      },
      metadataJson: "{}",
      contextWindowTokens: 500_000,
      inputLimitTokens: undefined,
      outputTokenLimit: 128_000,
    };
    const cases: readonly ProviderStreamEvent[] = [
      event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH, {
        finish: { ...completeFinish, usage: undefined },
      }),
      event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH, {
        finish: { ...completeFinish, contextWindowTokens: undefined },
      }),
      event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH, {
        finish: { ...completeFinish, outputTokenLimit: undefined },
      }),
    ];

    for (const malformed of cases) {
      const service = createLLMService(gatewayClient([malformed]));
      expect(await collect(service.stream(request()))).toEqual([
        {
          type: "provider-error",
          error: expect.objectContaining({
            type: "runtime",
            code: "gateway_protocol_error",
            retryable: false,
            fatal: true,
          }),
        },
      ]);
    }
  });

  test("converts Gateway provider-error into bounded Runtime provider failure", async () => {
    const service = createLLMService(gatewayClient([
      event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR, {
        providerError: {
          metadataJson: "{}",
          error: {
            code: "provider_unavailable",
            message: "Provider Gateway lowering is not implemented in this stage.",
            retryable: true,
            fatal: false,
            statusCode: 503,
            retryAfterMs: 0,
          },
        },
      }),
    ]));

    expect(await collect(service.stream(request()))).toEqual([
      {
        type: "provider-error",
        error: expect.objectContaining({
          type: "provider",
          code: "provider_unavailable",
          retryable: true,
          fatal: false,
          statusCode: 503,
        }),
      },
    ]);
  });

  test("preserves Gateway provider-error taxonomy without collapsing to unknown", async () => {
    const gatewayCodes = [
      "credential_required",
      "credential_unavailable",
      "platform_keys_exhausted",
      "context_overflow",
      "provider_request_invalid",
      "provider_plan_required",
      "provider_key_unavailable",
      "provider_quota_exhausted",
    ] as const;

    for (const code of gatewayCodes) {
      const service = createLLMService(gatewayClient([
        event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR, {
          providerError: {
            metadataJson: "{}",
            error: {
              code,
              message: `${code} message`,
              retryable: false,
              fatal: true,
              statusCode: 400,
              retryAfterMs: 0,
            },
          },
        }),
      ]));

      expect(await collect(service.stream(request()))).toEqual([
        {
          type: "provider-error",
          error: expect.objectContaining({ type: "provider", code }),
        },
      ]);
    }
  });

  test("settles customer credential failures as exhausted without terminating the Session", async () => {
    for (const code of ["credential_required", "credential_unavailable"] as const) {
      const service = createLLMService(gatewayClient([
        event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR, {
          providerError: {
            metadataJson: "{}",
            error: {
              code,
              message: "Customer credential is unavailable.",
              retryable: false,
              fatal: true,
              statusCode: 401,
              retryAfterMs: 0,
            },
          },
        }),
      ]));

      expect(await collect(service.stream(request()))).toEqual([
        {
          type: "provider-error",
          error: expect.objectContaining({
            type: "provider",
            code,
            retryStatus: { type: "exhausted" },
          }),
        },
      ]);
    }
  });

  test("marks definitive credential and platform unavailability as current-turn exhaustion", async () => {
    for (const code of ["provider_key_unavailable", "provider_unavailable"] as const) {
      const service = createLLMService(gatewayClient([
        event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR, {
          providerError: {
            metadataJson: "{}",
            error: {
              code,
              message: "Provider request cannot continue.",
              retryable: false,
              fatal: code === "provider_key_unavailable",
              statusCode: 401,
              retryAfterMs: 0,
            },
          },
        }),
      ]));

      expect(await collect(service.stream(request()))).toEqual([
        {
          type: "provider-error",
          error: expect.objectContaining({
            type: "provider",
            code,
            retryStatus: { type: "exhausted" },
          }),
        },
      ]);
    }
  });

  test("maps one pre-stream attachment rejection report for an origin in the request", async () => {
    const providerRequest = {
      ...request(),
      attachments: [{
        transient: undefined,
        fileBacked: { sourceEventId: "sevt_file_1", fileId: "file_1" },
        mime: "image/png",
        filename: "plot.png",
      }],
    };
    const service = createLLMService(gatewayClient([
      event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_ATTACHMENT_REJECTIONS, {
        attachmentRejections: {
          rejections: [{
            transientAttachmentRef: undefined,
            fileBacked: { sourceEventId: "sevt_file_1", fileId: "file_1" },
            reason: ProviderAttachmentRejectionReason.PROVIDER_ATTACHMENT_REJECTION_REASON_DELETED,
          }],
        },
      }),
      successfulFinish(ProviderFinishReason.PROVIDER_FINISH_REASON_STOP),
    ]));

    expect(await collect(service.stream(providerRequest))).toEqual([
      {
        type: "attachment-rejections",
        rejections: [{
          origin: { type: "file-backed", sourceEventId: "sevt_file_1", fileId: "file_1" },
          reason: "deleted",
        }],
      },
      { type: "finish", finishReason: "stop", usage: emptyFinishUsage, modelLimits: defaultFinishLimits },
    ]);
  });

  test("accepts only the unique transient attachment ref carried by the request", async () => {
    const transient = {
      attachmentRef: "att_1",
      sourcePath: "mcp:github/plot.png",
      pageRange: "1-2",
      detail: "high",
    };
    const providerRequest = {
      ...request(),
      attachments: [{
        transient,
        fileBacked: undefined,
        mime: "image/png",
        filename: "plot.png",
      }],
    };
    const events = await collect(createLLMService(gatewayClient([
      event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_ATTACHMENT_REJECTIONS, {
        attachmentRejections: {
          rejections: [{
            transientAttachmentRef: "att_other",
            fileBacked: undefined,
            reason: ProviderAttachmentRejectionReason.PROVIDER_ATTACHMENT_REJECTION_REASON_DELETED,
          }],
        },
      }),
    ])).stream(providerRequest));

    expect(events).toEqual([{
      type: "provider-error",
      error: expect.objectContaining({ code: "gateway_protocol_error" }),
    }]);
  });

  test("rejects repeated, late, and unknown attachment rejection reports as gateway_protocol_error", async () => {
    const providerRequest = {
      ...request(),
      attachments: [{
        transient: undefined,
        fileBacked: { sourceEventId: "sevt_file_1", fileId: "file_1" },
        mime: "image/png",
        filename: "plot.png",
      }],
    };
    const rejection = event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_ATTACHMENT_REJECTIONS, {
      attachmentRejections: {
        rejections: [{
          transientAttachmentRef: undefined,
          fileBacked: { sourceEventId: "sevt_file_1", fileId: "file_1" },
          reason: ProviderAttachmentRejectionReason.PROVIDER_ATTACHMENT_REJECTION_REASON_DELETED,
        }],
      },
    });
    const unknown = event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_ATTACHMENT_REJECTIONS, {
      attachmentRejections: {
        rejections: [{
          transientAttachmentRef: undefined,
          fileBacked: { sourceEventId: "sevt_unknown", fileId: "file_unknown" },
          reason: ProviderAttachmentRejectionReason.PROVIDER_ATTACHMENT_REJECTION_REASON_DELETED,
        }],
      },
    });

    const mappedRejection: LLMEvent = {
      type: "attachment-rejections",
      rejections: [{
        origin: { type: "file-backed", sourceEventId: "sevt_file_1", fileId: "file_1" },
        reason: "deleted",
      }],
    };
    const testCases: Array<{
      readonly events: readonly ProviderStreamEvent[];
      readonly expectedPrefix: readonly LLMEvent[];
    }> = [
      {
        events: [rejection, rejection],
        expectedPrefix: [mappedRejection],
      },
      {
        events: [
          completeText(),
          rejection,
        ],
        expectedPrefix: [{ type: "text-complete", providerPartId: "text-1", eventId: textEventId, text: "hello" }],
      },
      {
        events: [unknown],
        expectedPrefix: [],
      },
    ];
    for (const testCase of testCases) {
      const events = await collect(createLLMService(gatewayClient(testCase.events)).stream(providerRequest));
      expect(events.slice(0, -1)).toEqual([...testCase.expectedPrefix]);
      expect(events.at(-1)).toEqual({
        type: "provider-error",
        error: expect.objectContaining({ code: "gateway_protocol_error" }),
      });
    }
  });

  test("rejects every retired fragment enum instead of reconstructing fragment lifecycles", async () => {
    // v1 fragment enum values are reserved in v2. Fragment/name lifecycle checks
    // now belong to Gateway's assembler; Runtime rejects the obsolete protocol.
    for (let retiredType = 1; retiredType <= 10; retiredType++) {
      const output = await collect(createLLMService(gatewayClient([
        event(retiredType as ProviderStreamEventType),
      ])).stream(request()));
      expect(output).toEqual([{
        type: "provider-error",
        error: expect.objectContaining({ type: "runtime", code: "gateway_protocol_error", retryable: false, fatal: true }),
      }]);
    }
  });

  test("rejects unmatched reasoning and events after a valid terminal candidate", async () => {
    for (const events of [
      [completeReasoning()],
      [successfulFinish(ProviderFinishReason.PROVIDER_FINISH_REASON_STOP), completeText()],
      [successfulFinish(ProviderFinishReason.PROVIDER_FINISH_REASON_STOP), event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR, {
        providerError: { metadataJson: "{}", error: { code: "provider_unavailable", message: "provider failed", retryable: true, fatal: false, statusCode: 503, retryAfterMs: 0 } },
      })],
    ]) {
      expect(await collect(createLLMService(gatewayClient(events)).stream(request()))).toEqual([{
        type: "provider-error",
        error: expect.objectContaining({ type: "runtime", code: "gateway_protocol_error", retryable: false, fatal: true }),
      }]);
    }
  });

  test("rejects duplicate complete identities and mismatched reasoning while preserving the valid prefix", async () => {
    const cases = [
      { events: [completeText(), completeText()], prefixTypes: ["text-complete"] },
      { events: [completeText(), completeText("other-part")], prefixTypes: ["text-complete"] },
      { events: [completeText(), thinkingStarted("reasoning-1", textEventId)], prefixTypes: ["text-complete"] },
      { events: [thinkingStarted(), thinkingStarted()], prefixTypes: ["thinking-started"] },
      { events: [thinkingStarted(), completeReasoning("other-part")], prefixTypes: ["thinking-started"] },
      { events: [thinkingStarted(), completeReasoning("reasoning-1", textEventId)], prefixTypes: ["thinking-started"] },
      { events: [thinkingStarted(), completeReasoning(), completeReasoning()], prefixTypes: ["thinking-started", "reasoning-complete"] },
      { events: [completeTool(), completeTool()], prefixTypes: ["tool-call-complete"] },
      { events: [completeTool(), completeTool("call-1", "other")], prefixTypes: ["tool-call-complete"] },
    ] satisfies ReadonlyArray<{ events: ProviderStreamEvent[]; prefixTypes: LLMEvent["type"][] }>;
    for (const { events, prefixTypes } of cases) {
      const output = await collect(createLLMService(gatewayClient(events)).stream(request()));
      expect(output.map(item => item.type)).toEqual([...prefixTypes, "provider-error"]);
      expect(output.at(-1)).toEqual({
        type: "provider-error",
        error: expect.objectContaining({ type: "runtime", code: "gateway_protocol_error", retryable: false, fatal: true }),
      });
    }
  });

  test("rejects successful finish while a ThinkingStarted has no matching completion", async () => {
    const output = await collect(createLLMService(gatewayClient([
      thinkingStarted(), successfulFinish(ProviderFinishReason.PROVIDER_FINISH_REASON_STOP),
    ])).stream(request()));
    expect(output.map(item => item.type)).toEqual(["thinking-started", "provider-error"]);
    expect(output.at(-1)).toEqual({
      type: "provider-error",
      error: expect.objectContaining({ type: "runtime", code: "gateway_protocol_error", retryable: false, fatal: true }),
    });
  });

  test("allows kind-scoped Provider IDs and preserves completion observation order", async () => {
    const output = await collect(createLLMService(gatewayClient([
      thinkingStarted("shared"), completeText("shared"), completeTool(), completeReasoning("shared"),
      successfulFinish(ProviderFinishReason.PROVIDER_FINISH_REASON_TOOL_CALLS),
    ])).stream(request()));
    expect(output.map(item => item.type)).toEqual(["thinking-started", "text-complete", "tool-call-complete", "reasoning-complete", "finish"]);
    expect(output[1]).toEqual({ type: "text-complete", providerPartId: "shared", eventId: textEventId, text: "hello" });
    expect(output[3]).toEqual({ type: "reasoning-complete", providerPartId: "shared", thinkingEventId, text: "thinking" });
  });

  test("preserves explicit provider failure after complete content and unmatched ThinkingStarted", async () => {
    const service = createLLMService(gatewayClient([
      completeText(),
      thinkingStarted(),
      completeTool(),
      event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR, {
        providerError: {
          metadataJson: "{}",
          error: {
            code: "provider_unavailable",
            message: "provider failed",
            retryable: true,
            fatal: false,
            statusCode: 503,
            retryAfterMs: 0,
          },
        },
      }),
    ]));

    const output = await collect(service.stream(request()));
    expect(output.map((item) => item.type)).toEqual([
      "text-complete",
      "thinking-started",
      "tool-call-complete",
      "provider-error",
    ]);
    expect(output.at(-1)).toEqual({
      type: "provider-error",
      error: expect.objectContaining({
        type: "provider",
        code: "provider_unavailable",
        retryable: true,
        fatal: false,
      }),
    });
    expect((output.at(-1) as Extract<LLMEvent, { readonly type: "provider-error" }>).error)
      .not.toHaveProperty("retryAfterMs");
  });

  test("rejects malformed ProviderStreamEvent payloads as gateway_protocol_error", async () => {
    const malformedEvents = [
      completeText("text-1", ""),
      completeText("text-1", "hello", "invalid-event-id"),
      completeReasoning("reasoning-1", thinkingEventId, "not-json"),
      completeTool("call-1", "lookup", "not-json"),
      event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_COMPLETE),
      event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_COMPLETE, {
        thinkingStarted: { providerPartId: "reasoning-1", eventId: thinkingEventId },
      }),
      event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_COMPLETE, {
        textComplete: { providerPartId: "text-1", text: "hello", eventId: textEventId },
        thinkingStarted: { providerPartId: "reasoning-1", eventId: thinkingEventId },
      }),
      event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR, {
        providerError: { error: undefined, metadataJson: "{}" },
      }),
      event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH, {
        finish: {
          reason: ProviderFinishReason.PROVIDER_FINISH_REASON_UNSPECIFIED,
          metadataJson: "{}",
        },
      }),
      event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH, {
        finish: {
          reason: ProviderFinishReason.PROVIDER_FINISH_REASON_STOP,
          usage: {
            inputTotalTokens: -1,
            inputUncachedTokens: 0,
            outputTotalTokens: 0,
            totalTokens: 0,
            providerUsageJson: "{}",
          },
          metadataJson: "{}",
        },
      }),
    ];

    for (const malformed of malformedEvents) {
      const service = createLLMService(gatewayClient([malformed]));
      expect(await collect(service.stream(request()))).toEqual([
        {
          type: "provider-error",
          error: expect.objectContaining({
            type: "runtime",
            code: "gateway_protocol_error",
            retryable: false,
            fatal: true,
          }),
        },
      ]);
    }

    const semanticObservations: unknown[] = [];
    const malformedTerminal = malformedEvents.at(-2)!;
    const observedService = createLLMService(gatewayClient([malformedTerminal]), {
      terminalObserved: (_request, terminalKind) => semanticObservations.push({ terminalKind }),
      streamClosed: (_request, completion, terminalCandidateExisted) => semanticObservations.push({ completion, terminalCandidateExisted }),
    });
    expect(await collect(observedService.stream(request()))).toEqual([
      expect.objectContaining({ type: "provider-error", error: expect.objectContaining({ code: "gateway_protocol_error" }) }),
    ]);
    expect(semanticObservations).toEqual([{
      completion: { outcome: "eof" },
      terminalCandidateExisted: false,
    }]);
  });

  test("emits one exact tool call from the complete Gateway frame", async () => {
    const service = createLLMService(gatewayClient([
      completeTool(),
      successfulFinish(ProviderFinishReason.PROVIDER_FINISH_REASON_TOOL_CALLS),
    ]));

    expect(await collect(service.stream(request()))).toEqual([
      {
        type: "tool-call-complete", id: "call-1", toolName: "lookup", input: { q: "hi" },
        inputPreview: {
          preview: "{\"q\":\"hi\"}", truncated: false,
        },
      },
      { type: "finish", finishReason: "tool-calls", usage: emptyFinishUsage, modelLimits: defaultFinishLimits },
    ]);
  });

  test("keeps exact tool input above the message preview bound", async () => {
    const input = { content: "x".repeat(9_000), file_path: "notes/large.txt" };
    const service = createLLMService(gatewayClient([
      completeTool("call-large", "Write", JSON.stringify(input)),
      successfulFinish(ProviderFinishReason.PROVIDER_FINISH_REASON_TOOL_CALLS),
    ]));

    const events = await collect(service.stream(request()));

    expect(events[0]).toMatchObject({
      type: "tool-call-complete",
      id: "call-large",
      toolName: "Write",
      input,
      inputPreview: {
        truncated: true,
      },
    });
    expect(events[0]?.type === "tool-call-complete" ? new TextEncoder().encode(events[0].inputPreview.preview).byteLength : 0).toBeLessThanOrEqual(8_192);
    expect(events[1]).toMatchObject({ type: "finish", finishReason: "tool-calls" });
  });

  test("emits gateway_stream_error when Gateway stream closes without terminal", async () => {
    expect(await collect(streamLLMEvents(request(), gatewayClient([])))).toEqual([
      {
        type: "provider-error",
        error: expect.objectContaining({
          type: "runtime",
          code: "gateway_stream_error",
        }),
      },
    ]);
  });

  test("discards a buffered terminal when transport completion fails", async () => {
    const failure: GatewayClientError = {
      type: "gateway-client",
      code: "gateway_unavailable",
      message: "Gateway provider stream is unavailable.",
      retryable: true,
      fatal: false,
    };
    const client: GatewayClient = {
      async streamProviderRequest() {
        return {
          events: Stream.fromIterable([successfulFinish(ProviderFinishReason.PROVIDER_FINISH_REASON_STOP)]),
          completion: Promise.resolve({ outcome: "transport_failure" as const, error: failure }),
          cancel: () => undefined,
        };
      },
    };

    const error = await Effect.runPromise(Stream.runCollect(createLLMService(client).stream(request())).pipe(Effect.flip));
    expect(error.error).toMatchObject({ code: "gateway_stream_error", retryable: true, fatal: false });
  });

  test("maps the call-start completion deadline through the existing retryable stream failure", async () => {
    const observations: unknown[] = [];
    const client: GatewayClient = {
      async streamProviderRequest() {
        return {
          events: Stream.empty,
          completion: Promise.resolve({ outcome: "completion_deadline" as const }),
          cancel: () => undefined,
        };
      },
    };
    const error = await Effect.runPromise(Stream.runCollect(createLLMService(client, {
      nowEpochMs: () => 10,
      streamClosed: (_request, completion, terminalCandidateExisted) => observations.push({ completion, terminalCandidateExisted }),
    }).stream(request())).pipe(Effect.flip));

    expect(error.error).toMatchObject({
      type: "runtime",
      code: "gateway_stream_error",
      reason: "gateway_transport_completion_deadline",
      retryable: true,
      fatal: false,
    });
    expect(observations).toEqual([{
      completion: { outcome: "completion_deadline" },
      terminalCandidateExisted: false,
    }]);
  });
});
