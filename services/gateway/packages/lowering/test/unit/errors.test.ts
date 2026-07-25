import { describe, expect, test } from "bun:test";
import { ProviderStreamEventType } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { ProviderStreamTimeoutError, classifyOpenAIProviderError, classifyProviderStreamError, providerErrorEvent } from "../../src/errors.js";

describe("Gateway provider error raising", () => {
  test("emits bounded provider-error events without raw provider material", () => {
    const event = providerErrorEvent({ requestId: "req_1", modelRequestId: "mreq_1" }, {
      code: "provider_unavailable",
      message: "Provider model is not enabled.",
      retryable: false,
      fatal: true,
      statusCode: 503,
    });

    expect(event).toMatchObject({
      requestId: "req_1",
      modelRequestId: "mreq_1",
      type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR,
      providerError: {
        metadataJson: "{}",
        error: {
          code: "provider_unavailable",
          retryable: false,
          fatal: true,
          statusCode: 503,
        },
      },
    });
    expect(JSON.stringify(event)).not.toContain("sk-");
  });

  test("classifies abort and generic stream failures for Runtime-owned retry decisions", () => {
    expect(classifyProviderStreamError(new DOMException("aborted", "AbortError"))).toMatchObject({
      code: "provider_cancelled",
      retryable: false,
      fatal: false,
    });
    expect(classifyProviderStreamError(new Error("socket closed"))).toMatchObject({
      code: "provider_stream_error",
      retryable: true,
      fatal: false,
    });
  });

  test("classifies provider stream timeouts as Runtime-retryable provider timeouts", () => {
    expect(classifyProviderStreamError(new ProviderStreamTimeoutError("inter_chunk_timeout"))).toMatchObject({
      code: "provider_timeout",
      message: "Provider stream stalled before the next chunk.",
      retryable: true,
      fatal: false,
      statusCode: 504,
    });
  });

  test("openai-error-mapping classifies OpenAI provider-specific retry and plan errors", () => {
    expect(classifyOpenAIProviderError({ statusCode: 404 })).toMatchObject({
      code: "provider_stream_error",
      retryable: true,
      fatal: false,
    });
    for (const providerCode of ["usage_not_included", "insufficient_quota", "invalid_prompt"] as const) {
      expect(classifyOpenAIProviderError({ statusCode: 400, providerCode }), providerCode).toMatchObject({
        retryable: false,
        fatal: true,
      });
    }
    for (const providerCode of ["server_is_overloaded", "server_error"] as const) {
      expect(classifyOpenAIProviderError({ statusCode: 400, providerCode }), providerCode).toMatchObject({
        code: "provider_stream_error",
        retryable: true,
        fatal: false,
      });
    }
  });
});
