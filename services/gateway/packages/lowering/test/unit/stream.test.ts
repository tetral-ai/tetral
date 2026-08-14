import { describe, expect, test } from "bun:test";
import {
  ProviderFinishReason,
  ProviderStreamEventType,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { ProviderStreamRaiser } from "../../src/stream.js";

const TestModelLimits = {
  contextWindowTokens: 500_000,
  inputLimitTokens: 372_000,
  outputTokenLimit: 128_000,
} as const;

describe("Gateway stream raising", () => {
  test("maps streaming text, reasoning, tool, finish, and drops raw chunks", () => {
    const raiser = new ProviderStreamRaiser(
      {
        usageWireFamily: "openai-wire",
        modelLimits: TestModelLimits,
      },
    );

    const events = [
      ...raiser.map({ type: "raw", value: { ignored: true } }),
      ...raiser.map({ type: "text-start", id: "txt_1" }),
      ...raiser.map({ type: "text-delta", id: "txt_1", textDelta: "hello" }),
      ...raiser.map({ type: "text-end", id: "txt_1" }),
      ...raiser.map({ type: "reasoning-start", id: "rsn_1", metadata: { anthropic: { signature: "sig" } } }),
      ...raiser.map({ type: "reasoning-delta", id: "rsn_1", delta: "thinking" }),
      ...raiser.map({ type: "reasoning-end", id: "rsn_1" }),
      ...raiser.map({ type: "tool-input-start", id: "tool_1", name: "Read" }),
      ...raiser.map({ type: "tool-input-delta", id: "tool_1", textDelta: "{\"path\":\"a\"}" }),
      ...raiser.map({ type: "tool-input-end", id: "tool_1" }),
      ...raiser.map({ type: "tool-call", id: "tool_1", name: "Read", input: { path: "a" } }),
      ...raiser.map({
        type: "finish-step",
        usage: { inputTokens: 3, outputTokens: 5, providerOnlyCounter: 7, apiKey: "sk-provider-secret" } as never,
      }),
      ...raiser.map({ type: "finish", finishReason: "tool-calls", usage: { inputTokens: 3, outputTokens: 5 } }),
    ];

    expect(events.map((event) => event.type)).toEqual([
      ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START,
      ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_DELTA,
      ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_END,
      ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_START,
      ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_DELTA,
      ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_END,
      ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_START,
      ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_DELTA,
      ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_END,
      ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_CALL,
      ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
    ]);
    expect(events[1]?.text?.text).toBe("hello");
    expect(events[3]?.reasoning?.metadataJson).toContain("signature");
    expect(events[9]?.toolCall?.inputJson).toBe("{\"path\":\"a\"}");
    expect(events[10]?.finish?.reason).toBe(ProviderFinishReason.PROVIDER_FINISH_REASON_TOOL_CALLS);
    expect(events[10]?.finish?.usage).toEqual({
      inputTotalTokens: 3,
      inputUncachedTokens: 3,
      outputTotalTokens: 5,
      totalTokens: 8,
      providerUsageJson: "{\"inputTokens\":3,\"outputTokens\":5,\"providerOnlyCounter\":7,\"apiKey\":\"[redacted]\"}",
    });
    expect(events[10]?.finish).toMatchObject({
      contextWindowTokens: 500_000,
      inputLimitTokens: 372_000,
      outputTokenLimit: 128_000,
    });
  });

  test("synthesizes missing fragment ids and rejects contradictory tool-call names", () => {
    const raiser = new ProviderStreamRaiser(
      { usageWireFamily: "openai-wire", modelLimits: TestModelLimits },
    );

    const [start] = raiser.map({ type: "text-start" });
    const [delta] = raiser.map({ type: "text-delta", textDelta: "x" });
    const [end] = raiser.map({ type: "text-end" });
    const [nextStart] = raiser.map({ type: "text-start" });

    expect(start?.text?.id).toBe("text_1");
    expect(delta?.text?.id).toBe("text_1");
    expect(end?.text?.id).toBe("text_1");
    expect(nextStart?.text?.id).toBe("text_2");

    const reasoningRaiser = new ProviderStreamRaiser(
      { usageWireFamily: "openai-wire", modelLimits: TestModelLimits },
    );
    const [reasoningStart] = reasoningRaiser.map({ type: "reasoning-start" });
    const [reasoningEnd] = reasoningRaiser.map({ type: "reasoning-end" });
    const [nextReasoningStart] = reasoningRaiser.map({ type: "reasoning-start" });
    expect(reasoningStart?.reasoning?.id).toBe("reasoning_1");
    expect(reasoningEnd?.reasoning?.id).toBe("reasoning_1");
    expect(nextReasoningStart?.reasoning?.id).toBe("reasoning_2");

    const implicitToolRaiser = new ProviderStreamRaiser(
      { usageWireFamily: "openai-wire", modelLimits: TestModelLimits },
    );
    const [toolStart] = implicitToolRaiser.map({ type: "tool-input-start", name: "Read" });
    const [toolEnd] = implicitToolRaiser.map({ type: "tool-input-end" });
    const [toolCall] = implicitToolRaiser.map({ type: "tool-call", name: "Read", input: { path: "a" } });
    const [nextToolStart] = implicitToolRaiser.map({ type: "tool-input-start", name: "Write" });
    expect(toolStart?.toolInput?.id).toBe("tool_1");
    expect(toolEnd?.toolInput?.id).toBe("tool_1");
    expect(toolCall?.toolCall?.id).toBe("tool_1");
    expect(nextToolStart?.toolInput?.id).toBe("tool_2");

    const toolRaiser = new ProviderStreamRaiser(
      { usageWireFamily: "openai-wire", modelLimits: TestModelLimits },
    );
    toolRaiser.map({ type: "tool-input-start", id: "tool_1", name: "Read" });
    expect(() => toolRaiser.map({ type: "tool-call", id: "tool_1", name: "Write", input: {} })).toThrow();
  });

  test("rejects events after terminal finish", () => {
    const raiser = new ProviderStreamRaiser(
      { usageWireFamily: "openai-wire", modelLimits: TestModelLimits },
    );

    raiser.map({ type: "finish", finishReason: "stop" });

    expect(() => raiser.map({ type: "text-start", id: "late" })).toThrow();
  });

  test("redacts raw provider metadata while preserving signed reasoning metadata", () => {
    const raiser = new ProviderStreamRaiser(
      { usageWireFamily: "openai-wire", modelLimits: TestModelLimits },
    );

    const [event] = raiser.map({
      type: "reasoning-start",
      id: "rsn_1",
      metadata: {
        anthropic: { signature: "sig_safe" },
        apiKey: "sk-live-secret",
        headers: { authorization: "Bearer provider-secret" },
        raw: { body: "raw prompt" },
        requestBody: "{\"messages\":[{\"content\":\"secret prompt\"}]}",
        stackTrace: "Error: boom\n at provider",
        signedUrl: "https://storage.example/signed?token=secret",
        note: "authorization: bearer token-secret",
      },
    });

    const metadata = JSON.parse(event?.reasoning?.metadataJson ?? "{}");
    expect(metadata.anthropic.signature).toBe("sig_safe");
    expect(metadata.apiKey).toBe("[redacted]");
    expect(metadata.headers).toBe("[redacted]");
    expect(metadata.raw).toBe("[redacted]");
    expect(metadata.requestBody).toBe("[redacted]");
    expect(metadata.stackTrace).toBe("[redacted]");
    expect(metadata.signedUrl).toBe("[redacted]");
    expect(metadata.note).toBe("[redacted]");
    expect(event?.reasoning?.metadataJson).not.toContain("sk-live-secret");
    expect(event?.reasoning?.metadataJson).not.toContain("provider-secret");
    expect(event?.reasoning?.metadataJson).not.toContain("secret prompt");
    expect(event?.reasoning?.metadataJson).not.toContain("https://storage.example");
  });

  test("bounds provider metadata JSON", () => {
    const raiser = new ProviderStreamRaiser(
      { usageWireFamily: "openai-wire", modelLimits: TestModelLimits },
    );

    const [event] = raiser.map({ type: "text-start", metadata: { payload: "x".repeat(17 * 1024) } });

    expect(event?.text?.metadataJson).toBe("{}");
  });
});
