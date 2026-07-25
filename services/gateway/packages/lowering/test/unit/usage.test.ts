import { describe, expect, test } from "bun:test";
import { normalizeProviderUsage } from "../../src/usage.js";

describe("Gateway provider usage normalization", () => {
  test("normalizes official Anthropic cache hits from noCacheTokens without double-counting cache reads", () => {
    const recordedSDKUsage = {
      inputTokens: 15616,
      outputTokens: 4,
      totalTokens: 15620,
      cachedInputTokens: 15614,
      inputTokenDetails: {
        noCacheTokens: 2,
        cacheReadTokens: 15614,
        cacheWriteTokens: 0,
      },
      outputTokenDetails: {},
    };
    const usage = normalizeProviderUsage(recordedSDKUsage, {
      wireFamily: "anthropic-wire",
      providerUsage: recordedSDKUsage,
    });

    expect(usage).toMatchObject({
      inputTotalTokens: 15616,
      inputUncachedTokens: 2,
      inputCacheReadTokens: 15614,
      inputCacheWriteTokens: 0,
      outputTotalTokens: 4,
      totalTokens: 15620,
    });
    expect(usage?.inputTotalTokens).toBe(recordedSDKUsage.inputTokens);
  });

  test("normalizes Kimi terminal-frame cache hits from noCacheTokens", () => {
    const recordedSDKUsage = {
      inputTokens: 8336,
      outputTokens: 21,
      totalTokens: 8357,
      cachedInputTokens: 8336,
      inputTokenDetails: {
        noCacheTokens: 0,
        cacheReadTokens: 8336,
        cacheWriteTokens: 0,
      },
      outputTokenDetails: {},
    };
    const usage = normalizeProviderUsage(recordedSDKUsage, {
      wireFamily: "anthropic-wire",
      providerUsage: recordedSDKUsage,
    });

    expect(usage).toMatchObject({
      inputTotalTokens: 8336,
      inputUncachedTokens: 0,
      inputCacheReadTokens: 8336,
      inputCacheWriteTokens: 0,
      outputTotalTokens: 21,
      totalTokens: 8357,
    });
    expect(usage?.inputTotalTokens).toBe(recordedSDKUsage.inputTokens);
  });

  test("normalizes anthropic-wire cache hits by deriving no-cache tokens when SDK details are absent", () => {
    const usage = normalizeProviderUsage({
      inputTokens: 16,
      outputTokens: 6,
      totalTokens: 22,
      inputTokenDetails: {
        cacheReadTokens: 4,
        cacheWriteTokens: 2,
      },
      outputTokenDetails: {
        reasoningTokens: 1,
      },
    }, {
      wireFamily: "anthropic-wire",
      providerUsage: { count: 10, apiKey: "sk-test-secret" },
    });

    expect(usage).toEqual({
      inputTotalTokens: 16,
      inputUncachedTokens: 10,
      inputCacheReadTokens: 4,
      inputCacheWriteTokens: 2,
      outputTotalTokens: 6,
      outputReasoningTokens: 1,
      totalTokens: 22,
      providerUsageJson: "{\"count\":10,\"apiKey\":\"[redacted]\"}",
    });
  });

  test("normalizes openai-wire cache hits by subtracting cache reads from SDK inputTokens", () => {
    const usage = normalizeProviderUsage({
      inputTokens: 10,
      outputTokens: 6,
      cachedInputTokens: 4,
      inputTokenDetails: {
        cacheWriteTokens: 2,
      },
      reasoningTokens: 3,
    }, {
      wireFamily: "openai-wire",
      providerUsage: { inputTokens: 10, authorization: "Bearer live-token" },
    });

    expect(usage).toEqual({
      inputTotalTokens: 12,
      inputUncachedTokens: 6,
      inputCacheReadTokens: 4,
      inputCacheWriteTokens: 2,
      outputTotalTokens: 6,
      outputReasoningTokens: 3,
      totalTokens: 18,
      providerUsageJson: "{\"inputTokens\":10,\"authorization\":\"[redacted]\"}",
    });
  });

  test("computes total tokens from normalized components and retains provider drift only in diagnostics", () => {
    const withProviderTotal = normalizeProviderUsage({
      inputTokens: 100,
      outputTokens: 20,
      totalTokens: 777,
      inputTokenDetails: {
        noCacheTokens: 60,
        cacheReadTokens: 40,
      },
    }, {
      wireFamily: "anthropic-wire",
    });
    const withoutProviderTotal = normalizeProviderUsage({
      inputTokens: 100,
      outputTokens: 20,
      inputTokenDetails: {
        noCacheTokens: 60,
        cacheReadTokens: 40,
      },
    }, {
      wireFamily: "anthropic-wire",
    });

    expect(withProviderTotal?.totalTokens).toBe(120);
    expect(withoutProviderTotal?.totalTokens).toBe(120);
    expect(withProviderTotal?.providerUsageJson).toContain('"totalTokens":777');
  });

  test("bounds opaque provider usage JSON", () => {
    const usage = normalizeProviderUsage({
      inputTokens: 1,
      outputTokens: 2,
    }, {
      wireFamily: "openai-wire",
      providerUsage: { payload: "x".repeat(17 * 1024) },
    });

    expect(usage?.providerUsageJson).toBe("{}");
  });

  test("redacts raw provider transport material from opaque usage JSON", () => {
    const usage = normalizeProviderUsage({
      inputTokens: 1,
      outputTokens: 2,
      cachedInputTokens: 0,
    }, {
      wireFamily: "openai-wire",
      providerUsage: {
        inputTokens: 1,
        outputTokens: 2,
        cacheReadTokens: 0,
        responseId: "resp_safe",
        key: "plain-secret",
        accessKey: "plain-access",
        providerKey: "plain-provider",
        headers: { authorization: "Bearer live-token" },
        raw: { prompt: "raw prompt text", response: "raw body" },
        requestBody: { prompt: "raw prompt text" },
        stackTrace: "Error: boom\n at provider",
        signedUrl: "https://example.com/object?signature=secret",
      },
    });

    expect(usage?.providerUsageJson).toBe(
      "{\"inputTokens\":1,\"outputTokens\":2,\"cacheReadTokens\":0,\"responseId\":\"resp_safe\",\"key\":\"[redacted]\",\"accessKey\":\"[redacted]\",\"providerKey\":\"[redacted]\",\"headers\":\"[redacted]\",\"raw\":\"[redacted]\",\"requestBody\":\"[redacted]\",\"stackTrace\":\"[redacted]\",\"signedUrl\":\"[redacted]\"}",
    );
  });
});
