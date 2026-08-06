import { describe, expect, test } from "bun:test";
import { ProviderStreamEventType } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { MaxProviderErrorMessageBytes } from "@tetral/gateway-protocol/src/bounds.js";
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

  test("emits valid UTF-8 at the exact provider-error message byte boundary", () => {
    const exact = `${"a".repeat(MaxProviderErrorMessageBytes - 3)}€`;
    const atLimit = providerErrorEvent({ requestId: "req_exact", modelRequestId: "mreq_exact" }, { message: exact });
    const overLimit = providerErrorEvent({ requestId: "req_over", modelRequestId: "mreq_over" }, { message: `${exact}b` });

    expect(Buffer.byteLength(atLimit.providerError?.error?.message ?? "", "utf8")).toBe(MaxProviderErrorMessageBytes);
    expect(atLimit.providerError?.error?.message).toBe(exact);
    expect(overLimit.providerError?.error?.message).toBe(exact);
    expect(() => new TextDecoder("utf-8", { fatal: true }).decode(new TextEncoder().encode(overLimit.providerError?.error?.message))).not.toThrow();
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
