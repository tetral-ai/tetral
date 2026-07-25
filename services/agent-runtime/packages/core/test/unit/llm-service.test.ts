import { describe, expect, test } from "bun:test";
import { Effect, Stream } from "effect";
import {
  ProviderAttachmentRejectionReason,
  ProviderFinishReason,
  ProviderRequestKind,
  ProviderStreamEventType,
  RuntimeMessageRole,
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
    modelRequestId: "model-request-1",
    requestKind: ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
    workspaceId: "workspace-1",
    sessionId: "session-1",
    sessionThreadId: "thread-1",
    parentThreadId: undefined,
    bindingId: "binding-1",
    bindingGeneration: 7,
    runtimeBindingToken: "runtime-binding-token-1",
    model: { providerId: "openai", modelId: "gpt-5.5", variant: "" },
    system: [
      {
        kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE,
        text: "base system",
        cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_STABLE,
      },
    ],
    messages: [
      {
        id: "message-1",
        role: RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_USER,
        status: "completed",
        origin: "user",
        parts: [{ id: "part-1", text: { text: "hello" } }],
      },
    ],
    tools: [],
    attachments: [],
    limits: { maxOutputTokens: 128, timeoutMs: 1000 },
  };
}

function gatewayClient(events: readonly ProviderStreamEvent[]): GatewayClient {
  return {
    streamProviderRequest() {
      return Stream.fromIterable(events);
    },
  };
}

function event(type: ProviderStreamEventType, payload: Omit<ProviderStreamEvent, "requestId" | "modelRequestId" | "type"> = {}): ProviderStreamEvent {
  return {
    requestId: "provider-request-1",
    modelRequestId: "model-request-1",
    type,
    ...payload,
  };
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
        streamProviderRequest() {
          return Stream.fail(failure);
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
      streamProviderRequest() {
        return Stream.fail({
          type: "gateway-client",
          code: "gateway_protocol_error",
          message: "Gateway rejected the provider request.",
          retryable: false,
          fatal: true,
        });
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
      event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START, { text: { id: "text-1", text: "", metadataJson: "{}" } }),
      event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_DELTA, { text: { id: "text-1", text: "hello", metadataJson: "{}" } }),
      event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_END, { text: { id: "text-1", text: "", metadataJson: "{}" } }),
      event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_START, { reasoning: { id: "reasoning-1", text: "", metadataJson: "{}" } }),
      event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_DELTA, { reasoning: { id: "reasoning-1", text: "thinking", metadataJson: "{}" } }),
      event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_END, { reasoning: { id: "reasoning-1", text: "", metadataJson: "{\"anthropic\":{\"signature\":\"sig_1\"}}" } }),
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
      { type: "text-start", id: "text-1" },
      { type: "text-delta", id: "text-1", text_delta: "hello" },
      { type: "text-end", id: "text-1" },
      { type: "reasoning-start", id: "reasoning-1" },
      { type: "reasoning-delta", id: "reasoning-1", text_delta: "thinking" },
      { type: "reasoning-end", id: "reasoning-1", providerMetadata: { anthropic: { signature: "sig_1" } } },
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
            transient: undefined,
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

  test("rejects transient attachment reports whose full origin differs from the request", async () => {
    const transient = {
      attachmentRef: "att_1",
      sourceToolUseEventId: "sevt_tool_1",
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
    const mismatches = [
      { ...transient, sourceToolUseEventId: "sevt_tool_other" },
      { ...transient, sourcePath: "mcp:github/other.png" },
      { ...transient, pageRange: "3-4" },
      { ...transient, detail: "low" },
    ];

    for (const reported of mismatches) {
      const events = await collect(createLLMService(gatewayClient([
        event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_ATTACHMENT_REJECTIONS, {
          attachmentRejections: {
            rejections: [{
              transient: reported,
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
    }
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
          transient: undefined,
          fileBacked: { sourceEventId: "sevt_file_1", fileId: "file_1" },
          reason: ProviderAttachmentRejectionReason.PROVIDER_ATTACHMENT_REJECTION_REASON_DELETED,
        }],
      },
    });
    const unknown = event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_ATTACHMENT_REJECTIONS, {
      attachmentRejections: {
        rejections: [{
          transient: undefined,
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
          event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START, {
            text: { id: "text-1", text: "", metadataJson: "{}" },
          }),
          rejection,
        ],
        expectedPrefix: [{ type: "text-start", id: "text-1" }],
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

  test("rejects wrong ids, out-of-order fragments, and events after terminal as gateway_protocol_error", async () => {
    for (const events of [
      [event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START, { requestId: "wrong", text: { id: "text-1", text: "", metadataJson: "{}" } } as Partial<ProviderStreamEvent>)],
      [event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START, { modelRequestId: "wrong", text: { id: "text-1", text: "", metadataJson: "{}" } } as Partial<ProviderStreamEvent>)],
      [event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_DELTA, { text: { id: "text-1", text: "orphan", metadataJson: "{}" } })],
      [
        event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH, { finish: { reason: ProviderFinishReason.PROVIDER_FINISH_REASON_STOP, metadataJson: "{}" } }),
        event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START, { text: { id: "late", text: "", metadataJson: "{}" } }),
      ],
      [
        event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH, { finish: { reason: ProviderFinishReason.PROVIDER_FINISH_REASON_STOP, metadataJson: "{}" } }),
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
      ],
    ] satisfies readonly (readonly ProviderStreamEvent[])[]) {
      const service = createLLMService(gatewayClient(events));
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

  test("rejects duplicate fragments, duplicate tool calls, and tool-input name mismatches", async () => {
    const cases = [
      {
        events: [
          event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START, { text: { id: "text-1", text: "", metadataJson: "{}" } }),
          event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START, { text: { id: "text-1", text: "", metadataJson: "{}" } }),
        ],
        prefixTypes: ["text-start"],
      },
      {
        events: [
          event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START, { text: { id: "text-1", text: "", metadataJson: "{}" } }),
          event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_END, { text: { id: "text-1", text: "", metadataJson: "{}" } }),
          event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_DELTA, { text: { id: "text-1", text: "after-end", metadataJson: "{}" } }),
        ],
        prefixTypes: ["text-start", "text-end"],
      },
      {
        events: [
          event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_CALL, {
            toolCall: { id: "call-1", name: "lookup", inputJson: "{\"q\":\"hi\"}", metadataJson: "{}" },
          }),
          event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_CALL, {
            toolCall: { id: "call-1", name: "lookup", inputJson: "{\"q\":\"hi\"}", metadataJson: "{}" },
          }),
        ],
        prefixTypes: ["tool-call"],
      },
      {
        events: [
          event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_START, {
            toolInput: { id: "call-1", name: "lookup", text: "", metadataJson: "{}" },
          }),
          event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_DELTA, {
            toolInput: { id: "call-1", name: "other", text: "{\"q\":\"hi\"}", metadataJson: "{}" },
          }),
        ],
        prefixTypes: ["tool-input-start"],
      },
      {
        events: [
          event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_START, {
            toolInput: { id: "call-1", name: "lookup", text: "", metadataJson: "{}" },
          }),
          event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_END, {
            toolInput: { id: "call-1", name: "other", text: "", metadataJson: "{}" },
          }),
        ],
        prefixTypes: ["tool-input-start"],
      },
      {
        events: [
          event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_START, {
            toolInput: { id: "call-1", name: "lookup", text: "", metadataJson: "{}" },
          }),
          event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_END, {
            toolInput: { id: "call-1", name: "lookup", text: "", metadataJson: "{}" },
          }),
          event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_CALL, {
            toolCall: { id: "call-1", name: "other", inputJson: "{\"q\":\"hi\"}", metadataJson: "{}" },
          }),
        ],
        prefixTypes: ["tool-input-start", "tool-input-end"],
      },
    ] satisfies ReadonlyArray<{ readonly events: readonly ProviderStreamEvent[]; readonly prefixTypes: readonly LLMEvent["type"][] }>;

    for (const { events, prefixTypes } of cases) {
      const service = createLLMService(gatewayClient(events));
      const output = await collect(service.stream(request()));
      expect(output.map((item) => item.type)).toEqual([
        ...prefixTypes,
        "provider-error",
      ]);
      expect(output.at(-1)).toEqual({
        type: "provider-error",
        error: expect.objectContaining({
          type: "runtime",
          code: "gateway_protocol_error",
          retryable: false,
          fatal: true,
        }),
      });
    }
  });

  test("rejects terminal provider events while any stream fragment is open", async () => {
    const terminalError = event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR, {
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
    });
    const terminalFinish = event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH, {
      finish: { reason: ProviderFinishReason.PROVIDER_FINISH_REASON_STOP, metadataJson: "{}" },
    });
    const cases = [
      {
        events: [
          event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START, { text: { id: "text-1", text: "", metadataJson: "{}" } }),
          terminalFinish,
        ],
        prefixTypes: ["text-start"],
      },
      {
        events: [
          event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_START, { reasoning: { id: "reasoning-1", text: "", metadataJson: "{}" } }),
          terminalError,
        ],
        prefixTypes: ["reasoning-start"],
      },
      {
        events: [
          event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_START, {
            toolInput: { id: "call-1", name: "lookup", text: "", metadataJson: "{}" },
          }),
          terminalFinish,
        ],
        prefixTypes: ["tool-input-start"],
      },
    ] satisfies ReadonlyArray<{ readonly events: readonly ProviderStreamEvent[]; readonly prefixTypes: readonly LLMEvent["type"][] }>;

    for (const { events, prefixTypes } of cases) {
      const service = createLLMService(gatewayClient(events));
      const output = await collect(service.stream(request()));
      expect(output.map((item) => item.type)).toEqual([
        ...prefixTypes,
        "provider-error",
      ]);
      expect(output.at(-1)).toEqual({
        type: "provider-error",
        error: expect.objectContaining({
          type: "runtime",
          code: "gateway_protocol_error",
          retryable: false,
          fatal: true,
        }),
      });
    }
  });

  test("rejects malformed ProviderStreamEvent payloads as gateway_protocol_error", async () => {
    const malformedEvents = [
      event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START, {
        text: { id: "text-1", text: "start-cannot-carry-delta", metadataJson: "{}" },
      }),
      event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_END, {
        reasoning: { id: "reasoning-1", text: "end-cannot-carry-delta", metadataJson: "{}" },
      }),
      event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_DELTA, {
        text: { id: "text-1", text: "hello", metadataJson: "not-json" },
      }),
      event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_DELTA),
      event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_DELTA, {
        reasoning: { id: "reasoning-1", text: "wrong-payload", metadataJson: "{}" },
      }),
      event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_DELTA, {
        text: { id: "text-1", text: "hello", metadataJson: "{}" },
        reasoning: { id: "reasoning-1", text: "extra-payload", metadataJson: "{}" },
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
  });

  test("starts tool execution only from a valid complete tool-call event", async () => {
    const service = createLLMService(gatewayClient([
      event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_START, {
        toolInput: { id: "call-1", name: "lookup", text: "", metadataJson: "{}" },
      }),
      event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_DELTA, {
        toolInput: { id: "call-1", name: "lookup", text: "{\"q\":\"hi\"}", metadataJson: "{}" },
      }),
      event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_END, {
        toolInput: { id: "call-1", name: "lookup", text: "", metadataJson: "{}" },
      }),
      event(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_CALL, {
        toolCall: { id: "call-1", name: "lookup", inputJson: "{\"q\":\"hi\"}", metadataJson: "{}" },
      }),
      successfulFinish(ProviderFinishReason.PROVIDER_FINISH_REASON_TOOL_CALLS),
    ]));

    expect(await collect(service.stream(request()))).toEqual([
      { type: "tool-input-start", id: "call-1", toolName: "lookup" },
      { type: "tool-input-delta", id: "call-1", toolName: "lookup", text_delta: "{\"q\":\"hi\"}" },
      { type: "tool-input-end", id: "call-1", toolName: "lookup" },
      { type: "tool-call", id: "call-1", toolName: "lookup", input: { value: { q: "hi" }, preview: "{\"q\":\"hi\"}", truncated: false } },
      { type: "finish", finishReason: "tool-calls", usage: emptyFinishUsage, modelLimits: defaultFinishLimits },
    ]);
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
});
