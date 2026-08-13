import { describe, expect, test } from "bun:test";
import { createHmac } from "node:crypto";
import { readFile } from "node:fs/promises";
import { Metadata, status } from "@grpc/grpc-js";
import {
  ProviderAttachmentRejectionReason,
  ProviderFinishReason,
  ProviderRequestKind,
  ProviderStreamEventType,
  RunWebStatus,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { createRuntimeBindingTokenVerifier } from "@tetral/gateway-protocol/src/binding-token.js";
import { GrpcStatusError } from "../../src/errors.js";
import { BridgeAPIAttachmentResolver } from "../../src/attachments.js";
import { ProviderCredentialResolver } from "../../src/providers/credentials.js";
import { ProviderClientRegistry } from "../../src/providers/clients.js";
import { encryptAES256GCM } from "../../src/providers/crypto.js";
import { ProviderKeyFailureError } from "../../src/providers/pool.js";
import { ProviderRequestLoweringError } from "@tetral/gateway-lowering/src/errors.js";
import { ProviderGatewayServiceShell } from "../../src/service.js";
import { validFileBackedProviderAttachment, validProviderAttachment, validProviderRequest, validRunWebRequest } from "./fixtures.js";
import type { GatewayLogger } from "../../src/logger.js";
import type { GatewayAuthenticator, ProviderAttachmentResolver, ProviderRequestStreamer } from "../../src/service.js";
import type { RuntimeBindingRequestIdentity, RuntimeBindingTokenVerifier } from "@tetral/gateway-protocol/src/binding-token.js";
import type { ProviderStreamEvent } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { GatewayCredentialStore, PlatformCredentialPool } from "../../src/providers/credentials.js";
import type { EncryptedPlatformProviderKeyRow, PlatformHostedProviderId, PlatformKeySelectionOptions, ProviderFailureClassification } from "../../src/providers/pool.js";

const RuntimePodUid = "pod_uid_gateway_service";
const BindingTokenKey = "gateway-runtime-binding-token-test-key-32";
const CredentialMasterKeyHex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const approvalReviewerOutputSchemaJson = await readFile(
  new URL("../../../../../agent-runtime/packages/runtime-pod/src/assets/approval-reviewer-output-schema.json", import.meta.url),
  "utf8",
);

describe("ProviderGatewayServiceShell", () => {
  test("authorizes before request validation or provider-unavailable response construction", async () => {
    const authenticator = new RecordingAuthenticator({ ok: false, code: "Unauthenticated", message: "unauthenticated" });
    const service = createService(authenticator);

    await expectGrpcCode(collectEvents(service.streamProviderRequest({ ...validProviderRequest(), requestId: "" }, metadata())), status.UNAUTHENTICATED);

    expect(authenticator.calls).toEqual(["/tetral.provider_gateway.v1.ProviderGatewayService/StreamProviderRequest"]);
  });

  test("rejects invalid request-kind output-schema combinations before credential resolution", async () => {
    const pool = new RecordingPlatformCredentialPool(["pfk_reviewer"]);
    const service = createService(new RecordingAuthenticator(), true, { verify: () => true }, {
      credentialResolver: platformCredentialResolver(pool),
    });
    const malformedRequests = [
      validProviderRequest({
        requestKind: ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
        model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "" },
        outputSchemaJson: undefined,
      }),
      validProviderRequest({ outputSchemaJson: approvalReviewerOutputSchemaJson }),
      validProviderRequest({
        requestKind: ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
        model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "" },
        outputSchemaJson: "not-json",
      }),
    ];

    for (const request of malformedRequests) {
      await expectGrpcCode(collectEvents(service.streamProviderRequest(request, metadata())), status.INVALID_ARGUMENT);
    }
    expect(pool.selectCalls).toBe(0);
  });

  test("invalid requests log each independently bounded identity and validation class", async () => {
    const hostileIdentity = `https://example.invalid/${"x".repeat(300)}`;
    const infoLogs: unknown[] = [];
    const errorLogs: unknown[] = [];
    const service = createService(new RecordingAuthenticator(), true, { verify: () => true }, {
      logger: { info: (record) => infoLogs.push(record), error: (record) => errorLogs.push(record) },
    });

    await expectGrpcCode(collectEvents(service.streamProviderRequest(
      validProviderRequest({ workspaceId: hostileIdentity }),
      metadata(),
    )), status.INVALID_ARGUMENT);

    expect(infoLogs).toEqual([]);
    expect(JSON.stringify(errorLogs)).not.toContain(hostileIdentity);
    expect(errorLogs).toEqual([
      expect.objectContaining({
        event: "provider_request_streamed",
        "request.outcome": "failed",
        "error.class": "request_validation",
        "error.code": "invalid_identifier",
        "validation.member": "workspace_id",
        "session.id": "sesn_1",
        "thread.id": "thrd_1",
        "request.id": "req_1",
        "model_request.id": "mreq_1",
      }),
    ]);
    expect(errorLogs[0]).not.toHaveProperty("workspace.id");
  });

  test("valid ProviderRequest streams catalog-gated provider-unavailable terminal event", async () => {
    const request = validProviderRequest({
      runtimeBindingToken: signedRuntimeBindingToken(validProviderRequest(), RuntimePodUid),
    });
    const service = createService(
      new RecordingAuthenticator(),
      true,
      createRuntimeBindingTokenVerifier({
        hmacKey: BindingTokenKey,
        now: () => new Date("2026-01-01T00:00:00Z"),
      }),
    );

    const events = await collectEvents(service.streamProviderRequest(request, metadata()));

    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
      requestId: request.requestId,
      modelRequestId: request.modelRequestId,
      type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR,
      providerError: {
        error: {
          code: "provider_unavailable",
          retryable: false,
          fatal: true,
        },
      },
    });
  });

  test("rejects malformed provider stream events before emitting them downstream", async () => {
    const base = validProviderRequest({ model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "" } });
    const request = validProviderRequest({
      ...base,
      runtimeBindingToken: signedRuntimeBindingToken(base, RuntimePodUid),
    });
    const service = createService(new RecordingAuthenticator(), true, { verify: () => true }, {
      providerStreamer: {
        stream: async function* () {
          yield textEvent(request, ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START, "not-a-delta");
        },
      },
    });

    await expectGrpcCode(collectEvents(service.streamProviderRequest(request, metadata())), status.INTERNAL);
  });

  test("provider failure logs reject URL-shaped lower-layer error codes", async () => {
    const hostileCode = `https://provider.invalid/${"x".repeat(300)}`;
    const logs: unknown[] = [];
    const request = validProviderRequest({
      model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "" },
    });
    const service = createService(new RecordingAuthenticator(), true, { verify: () => true }, {
      logger: { info: (record) => logs.push(record), error: (record) => logs.push(record) },
      credentialResolver: sessionCredentialResolver(),
      providerStreamer: {
        stream: async function* () {
          throw new ProviderRequestLoweringError({
            code: hostileCode,
            message: "Provider request could not be lowered.",
            retryable: false,
            fatal: true,
          });
        },
      },
    });

    await collectEvents(service.streamProviderRequest(request, metadata()));

    expect(JSON.stringify(logs)).not.toContain(hostileCode);
    expect(logs).toEqual([
      expect.objectContaining({
        event: "provider_request_streamed",
        "request.outcome": "failed",
        "error.class": "provider_error",
        "error.code": "provider_error",
      }),
    ]);
  });

  test("logs local capability rejection as configuration failure", async () => {
    const logs: unknown[] = [];
    let providerFactoryCalls = 0;
    let platformFailureRecords = 0;
    const providerStreamer = new ProviderClientRegistry({
      openAIProviderFactory: () => {
        providerFactoryCalls += 1;
        return { responses: (modelId) => ({ provider: "openai", modelId }) };
      },
    });
    const request = validProviderRequest({
      requestKind: ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
      model: { providerId: "openai", modelId: "gpt-5.5", variant: "" },
      outputSchemaJson: approvalReviewerOutputSchemaJson,
    });
    const service = createService(new RecordingAuthenticator(), true, { verify: () => true }, {
      logger: { info: (record) => logs.push(record), error: (record) => logs.push(record) },
      credentialResolver: platformOpenAICredentialResolver(() => {
        platformFailureRecords += 1;
      }),
      providerStreamer,
    });

    const events = await collectEvents(service.streamProviderRequest(request, metadata()));

    expect(events).toEqual([
      expect.objectContaining({
        type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR,
        providerError: expect.objectContaining({
          error: expect.objectContaining({ code: "provider_configuration_invalid", retryable: false, fatal: true }),
        }),
      }),
    ]);
    expect(logs).toEqual([
      expect.objectContaining({
        event: "provider_request_streamed",
        "request.outcome": "failed",
        "error.class": "provider_configuration",
        "error.code": "provider_configuration_invalid",
        "provider.id": "openai",
        "model.id": "gpt-5.5",
        "request.id": request.requestId,
      }),
    ]);
    expect(providerFactoryCalls).toBe(0);
    expect(platformFailureRecords).toBe(0);
    expect(JSON.stringify(logs)).not.toContain("additionalProperties");
    expect(JSON.stringify(logs)).not.toContain("sk-platform-openai");
  });

  test("stream and web logs emit shared safe fields", async () => {
    const logs: unknown[] = [];
    const base = validProviderRequest({ model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "" } });
    const request = validProviderRequest({
      ...base,
      runtimeBindingToken: signedRuntimeBindingToken(base, RuntimePodUid),
    });
    const service = createService(
      new RecordingAuthenticator(),
      true,
      createRuntimeBindingTokenVerifier({
        hmacKey: BindingTokenKey,
        now: () => new Date("2026-01-01T00:00:00Z"),
      }),
      {
        logger: { info: (record) => logs.push(record), error: (record) => logs.push(record) },
      },
    );

    const webBase = validRunWebRequest();
    const webRequest = {
      ...webBase,
      runtimeBindingToken: signedRuntimeBindingToken(webBase, RuntimePodUid),
    };

    await collectEvents(service.streamProviderRequest(request, metadata()));
    await service.runWeb(webRequest, metadata()).catch(() => undefined);

    expect(logs).toEqual([
      expect.objectContaining({
        event: "provider_request_streamed",
        "event.kind": "provider_request_streamed",
        operation: "StreamProviderRequest",
        component: "gateway",
        "request.outcome": "failed",
        "error.class": "provider_error",
        "error.code": "provider_unavailable",
        "request.id": request.requestId,
        "model_request.id": request.modelRequestId,
        "duration.ms": expect.any(Number),
      }),
      expect.objectContaining({
        event: "gateway_web_misrouted",
        "event.kind": "gateway_web_misrouted",
        operation: "RunWeb",
        component: "gateway",
        "error.class": "runtime_error",
        "error.code": "web_not_served_here",
        "error.message_safe": "web execution is served by the web-connector port",
      }),
    ]);
  });

  test("compaction carries the session credential while approval review carries the platform credential", async () => {
    const openAIFixture = await readFile(
      new URL("../golden/fixtures/openai-gpt-5.5-responses-live-2026-07-06.sse", import.meta.url),
      "utf8",
    );
    const anthropicFixture = await readFile(
      new URL("../golden/fixtures/anthropic-claude-opus-4-8-live-2026-07-03.sse", import.meta.url),
      "utf8",
    );
    const observedCredentialHeaders: Array<{ readonly name: string; readonly value: string }> = [];
    const providerStreamer = new ProviderClientRegistry({
      fetch: Object.assign(async (input: RequestInfo | URL, init?: RequestInit) => {
        const request = new Request(input, init);
        const anthropic = request.url.startsWith("https://api.anthropic.com/");
        const name = anthropic ? "x-api-key" : "authorization";
        observedCredentialHeaders.push({ name, value: request.headers.get(name) ?? "" });
        return new Response(anthropic ? anthropicFixture : openAIFixture, {
          status: 200,
          headers: { "content-type": "text/event-stream" },
        });
      }, { preconnect: () => undefined }),
    });
    const pool = new RecordingPlatformCredentialPool(["pfk_wire_only"]);
    const resolverWithSession = await productionCredentialResolver(pool, true);
    const resolverWithoutSession = await productionCredentialResolver(pool, false);
    const compactionService = createService(new RecordingAuthenticator(), true, { verify: () => true }, {
      credentialResolver: resolverWithSession,
      providerStreamer,
    });
    const platformCompactionService = createService(new RecordingAuthenticator(), true, { verify: () => true }, {
      credentialResolver: resolverWithoutSession,
      providerStreamer,
    });
    const reviewerService = createService(new RecordingAuthenticator(), true, { verify: () => true }, {
      credentialResolver: resolverWithSession,
      providerStreamer,
    });
    const compactionBase = validProviderRequest({
      requestKind: ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY,
      model: { providerId: "openai", modelId: "gpt-5.5", variant: "" },
      tools: [],
    });
    const reviewerBase = validProviderRequest({
      requestKind: ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
      model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "" },
      outputSchemaJson: approvalReviewerOutputSchemaJson,
    });

    const sessionCompactionEvents = await collectEvents(compactionService.streamProviderRequest({
      ...compactionBase,
      runtimeBindingToken: signedRuntimeBindingToken(compactionBase, RuntimePodUid),
    }, metadata()));
    const platformCompactionEvents = await collectEvents(platformCompactionService.streamProviderRequest({
      ...compactionBase,
      requestId: "req_compaction_platform",
      modelRequestId: "mreq_compaction_platform",
      runtimeBindingToken: signedRuntimeBindingToken(compactionBase, RuntimePodUid),
    }, metadata()));
    const reviewerEvents = await collectEvents(reviewerService.streamProviderRequest({
      ...reviewerBase,
      runtimeBindingToken: signedRuntimeBindingToken(reviewerBase, RuntimePodUid),
    }, metadata()));

    expect([
      sessionCompactionEvents.at(-1)?.type,
      platformCompactionEvents.at(-1)?.type,
      reviewerEvents.at(-1)?.type,
    ]).toEqual([
      ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
      ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
      ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
    ]);
    expect(observedCredentialHeaders).toEqual([
      { name: "authorization", value: "Bearer sk-session-openai" },
      { name: "authorization", value: "Bearer sk-pfk_wire_only" },
      { name: "x-api-key", value: "sk-pfk_wire_only" },
    ]);
  });

  test("admission cap fails fast with retryable provider-error without queueing", async () => {
    const base = validProviderRequest({ model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "" } });
    const request = validProviderRequest({
      ...base,
      runtimeBindingToken: signedRuntimeBindingToken(base, RuntimePodUid),
    });
    let started!: () => void;
    const firstStarted = new Promise<void>((resolve) => {
      started = resolve;
    });
    const logs: unknown[] = [];
    const service = createService(new RecordingAuthenticator(), true, { verify: () => true }, {
      maxConcurrentTurns: 1,
      logger: { info: (record) => logs.push(record), error: (record) => logs.push(record) },
      providerStreamer: {
        stream: async function* () {
          started();
          await new Promise((resolve) => setTimeout(resolve, 50));
          yield providerError(request, "provider_unavailable", false);
        },
      },
    });

    const first = collectEvents(service.streamProviderRequest(request, metadata()));
    await firstStarted;
    const second = await collectEvents(service.streamProviderRequest({
      ...request,
      requestId: "req_2",
      modelRequestId: "mreq_2",
    }, metadata()));

    expect(second).toHaveLength(1);
    expect(second[0]?.providerError?.error).toMatchObject({
      code: "provider_unavailable",
      retryable: true,
      fatal: false,
    });
    await first;
    expect(logs.filter((record) =>
      (record as { readonly event?: unknown }).event === "provider_request_streamed"
      && (record as { readonly "model_request.id"?: unknown })["model_request.id"] === "mreq_2"
    )).toEqual([
      expect.objectContaining({
        "request.outcome": "failed",
        "error.class": "provider_error",
        "error.code": "provider_unavailable",
      }),
    ]);
  });

  test("first normalized event timeout produces a retryable terminal provider-error", async () => {
    const logs: unknown[] = [];
    const base = validProviderRequest({ model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "" } });
    let aborted = false;
    let release!: () => void;
    const request = validProviderRequest({
      ...base,
      runtimeBindingToken: signedRuntimeBindingToken(base, RuntimePodUid),
      limits: { maxOutputTokens: 1024, timeoutMs: 1_000 },
    });
    const service = createService(new RecordingAuthenticator(), true, { verify: () => true }, {
      logger: { info: (record) => logs.push(record), error: (record) => logs.push(record) },
      providerStreamTimeouts: { firstByteTimeoutMs: 5, interChunkTimeoutMs: 500 },
      providerStreamer: {
        stream: async function* (input) {
          input.abortSignal?.addEventListener("abort", () => {
            aborted = true;
            release();
          }, { once: true });
          await new Promise<void>((resolve) => {
            release = resolve;
          });
        },
      },
    });

    const events = await collectEvents(service.streamProviderRequest(request, metadata()));

    expect(events).toHaveLength(1);
    expect(events[0]?.providerError?.error).toMatchObject({
      code: "provider_timeout",
      message: "Provider did not start streaming before the timeout.",
      retryable: true,
      fatal: false,
      statusCode: 504,
    });
    expect(aborted).toBe(true);
    expect(logs.filter((record) => (record as { readonly event?: unknown }).event === "gateway_provider_timeout")).toEqual([
      expect.objectContaining({
        "timeout.kind": "first_event",
        "timeout.elapsed_ms": expect.any(Number),
        "error.class": "provider_timeout",
        "error.code": "provider_timeout",
      }),
    ]);
  });

  test("inter-chunk timeout produces a retryable terminal provider-error after prior progress", async () => {
    const logs: unknown[] = [];
    const base = validProviderRequest({ model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "" } });
    let aborted = false;
    let release!: () => void;
    const request = validProviderRequest({
      ...base,
      runtimeBindingToken: signedRuntimeBindingToken(base, RuntimePodUid),
      limits: { maxOutputTokens: 1024, timeoutMs: 1_000 },
    });
    const service = createService(new RecordingAuthenticator(), true, { verify: () => true }, {
      logger: {
        info: () => undefined,
        error: (record) => {
          logs.push(record);
          throw new Error("logging sink unavailable");
        },
      },
      providerStreamTimeouts: { firstByteTimeoutMs: 500, interChunkTimeoutMs: 5 },
      providerStreamer: {
        stream: async function* (input) {
          yield textEvent(request, ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START, "");
          input.abortSignal?.addEventListener("abort", () => {
            aborted = true;
            release();
          }, { once: true });
          await new Promise<void>((resolve) => {
            release = resolve;
          });
        },
      },
    });

    const events = await collectEvents(service.streamProviderRequest(request, metadata()));

    expect(events).toHaveLength(2);
    expect(events[0]?.type).toBe(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START);
    expect(events[1]?.providerError?.error).toMatchObject({
      code: "provider_timeout",
      message: "Provider stream stalled before the next chunk.",
      retryable: true,
      fatal: false,
      statusCode: 504,
    });
    expect(aborted).toBe(true);
    expect(logs).toEqual([
      expect.objectContaining({
        event: "gateway_provider_timeout",
        "timeout.kind": "inter_event",
        "timeout.elapsed_ms": expect.any(Number),
        "timeout.inter_event_gap_ms": expect.any(Number),
        "stream.last_event.kind": "text_start",
      }),
      expect.objectContaining({ event: "provider_request_streamed" }),
    ]);
    expect(JSON.stringify(logs)).not.toContain("Provider stream stalled before the next chunk.");
  });

  test("abort mid-body errors the provider stream instead of hanging", async () => {
    const base = validProviderRequest({ model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "" } });
    const request = validProviderRequest({
      ...base,
      runtimeBindingToken: signedRuntimeBindingToken(base, RuntimePodUid),
      limits: { maxOutputTokens: 1024, timeoutMs: 1_000 },
    });
    const abortController = new AbortController();
    const service = createService(new RecordingAuthenticator(), true, { verify: () => true }, {
      providerStreamTimeouts: { firstByteTimeoutMs: 500, interChunkTimeoutMs: 500 },
      providerStreamer: {
        stream: async function* () {
          yield textEvent(request, ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START, "");
          await never();
        },
      },
    });
    const iterator = service.streamProviderRequest(request, metadata(), { abortSignal: abortController.signal })[Symbol.asyncIterator]();

    const first = await iterator.next();
    abortController.abort();
    const second = await iterator.next();
    const third = await iterator.next();

    expect(first.value?.type).toBe(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START);
    expect(second.value?.providerError?.error).toMatchObject({
      code: "provider_cancelled",
      retryable: false,
      fatal: false,
      statusCode: 499,
    });
    expect(third.done).toBe(true);
    await iterator.return?.();
  });

  test("consumer cancellation closes the provider stream and records one failed outcome", async () => {
    const base = validProviderRequest({ model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "" } });
    const request = validProviderRequest({
      ...base,
      runtimeBindingToken: signedRuntimeBindingToken(base, RuntimePodUid),
    });
    const logs: unknown[] = [];
    let providerClosed = false;
    const service = createService(new RecordingAuthenticator(), true, { verify: () => true }, {
      logger: { info: (record) => logs.push(record), error: (record) => logs.push(record) },
      providerStreamer: {
        stream: async function* () {
          try {
            yield textEvent(request, ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START, "");
            await never();
          } finally {
            providerClosed = true;
          }
        },
      },
    });
    const iterator = service.streamProviderRequest(request, metadata())[Symbol.asyncIterator]();

    expect((await iterator.next()).value?.type).toBe(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START);
    await iterator.return?.();

    expect(providerClosed).toBe(true);
    expect(logs).toEqual([
      expect.objectContaining({
        event: "provider_request_streamed",
        "request.outcome": "failed",
        "error.class": "runtime_error",
        "error.code": "stream_incomplete",
      }),
    ]);
  });

  test("T-POOL-9 switches platform keys only before the first downstream event and caps attempts at three", async () => {
    const base = validProviderRequest({ model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "" } });
    const request = validProviderRequest({
      ...base,
      runtimeBindingToken: signedRuntimeBindingToken(base, RuntimePodUid),
    });
    const attempts: string[] = [];
    const pool = new RecordingPlatformCredentialPool(["pfk_1", "pfk_2", "pfk_3", "pfk_4"]);
    const service = createService(new RecordingAuthenticator(), true, { verify: () => true }, {
      credentialResolver: platformCredentialResolver(pool),
      providerStreamer: {
        stream: async function* (input) {
          attempts.push(input.credential?.source === "platform" ? input.credential.platformKey.keyId : "missing");
          throw new ProviderKeyFailureError(retryableProviderFailure());
        },
      },
    });

    const events = await collectEvents(service.streamProviderRequest(request, metadata()));

    expect(attempts).toEqual(["pfk_1", "pfk_2", "pfk_3"]);
    expect(pool.recordedFailures.map((failure) => failure.keyId)).toEqual(["pfk_1", "pfk_2", "pfk_3"]);
    expect(events).toHaveLength(1);
    expect(events[0]?.providerError?.error).toMatchObject({
      code: "provider_stream_error",
      retryable: true,
      fatal: false,
    });
  });

  test("T-POOL-2 completes one downstream sequence with the second key after a pre-byte 429", async () => {
    const base = validProviderRequest({ model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "" } });
    const request = validProviderRequest({
      ...base,
      runtimeBindingToken: signedRuntimeBindingToken(base, RuntimePodUid),
    });
    const attempts: string[] = [];
    const pool = new RecordingPlatformCredentialPool(["pfk_rate_limited", "pfk_success"]);
    const service = createService(new RecordingAuthenticator(), true, { verify: () => true }, {
      credentialResolver: platformCredentialResolver(pool),
      providerStreamer: {
        stream: async function* (input) {
          const keyID = input.credential?.source === "platform" ? input.credential.platformKey.keyId : "missing";
          attempts.push(keyID);
          if (keyID === "pfk_rate_limited") {
            throw new ProviderKeyFailureError(retryableProviderFailure());
          }
          yield textEvent(request, ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START, "");
          yield textEvent(request, ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_END, "");
          yield finishEvent(request);
        },
      },
    });

    const events = await collectEvents(service.streamProviderRequest(request, metadata()));

    expect(attempts).toEqual(["pfk_rate_limited", "pfk_success"]);
    expect(pool.recordedFailures.map((failure) => failure.keyId)).toEqual(["pfk_rate_limited"]);
    expect(events.map((event) => event.type)).toEqual([
      ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START,
      ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_END,
      ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
    ]);
  });

  test("T-POOL-9 switches platform keys for direct pre-byte provider stream throws", async () => {
    const base = validProviderRequest({ model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "" } });
    const request = validProviderRequest({
      ...base,
      runtimeBindingToken: signedRuntimeBindingToken(base, RuntimePodUid),
    });
    const attempts: string[] = [];
    const logs: unknown[] = [];
    const pool = new RecordingPlatformCredentialPool(["pfk_1", "pfk_2", "pfk_3", "pfk_4"]);
    const service = createService(new RecordingAuthenticator(), true, { verify: () => true }, {
      logger: { info: (record) => logs.push(record), error: (record) => logs.push(record) },
      credentialResolver: platformCredentialResolver(pool),
      providerStreamer: {
        stream: async function* (input) {
          attempts.push(input.credential?.source === "platform" ? input.credential.platformKey.keyId : "missing");
          throw new TypeError("network down before first byte secret-provider-body");
        },
      },
    });

    const events = await collectEvents(service.streamProviderRequest(request, metadata()));

    expect(attempts).toEqual(["pfk_1", "pfk_2", "pfk_3"]);
    expect(pool.recordedFailures.map((failure) => failure.keyId)).toEqual(["pfk_1", "pfk_2", "pfk_3"]);
    expect(pool.recordedFailures.map((failure) => failure.classification.providerError.code)).toEqual([
      "provider_stream_error",
      "provider_stream_error",
      "provider_stream_error",
    ]);
    expect(events).toHaveLength(1);
    expect(events[0]?.providerError?.error).toMatchObject({
      code: "provider_stream_error",
      retryable: true,
      fatal: false,
    });
    expect(logs).toEqual([
      expect.objectContaining({
        event: "provider_request_streamed",
        "error.class": "provider_transport_failure",
        "error.code": "provider_stream_error",
        "provider.id": "anthropic",
        "model.id": "claude-opus-4-8",
        "request.id": request.requestId,
      }),
    ]);
    expect(JSON.stringify(logs)).not.toContain("secret-provider-body");
    expect(JSON.stringify(logs)).not.toContain("sk-pfk_");
  });

  test("T-POOL-4 does not switch platform keys after a downstream event has been emitted", async () => {
    const base = validProviderRequest({ model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "" } });
    const request = validProviderRequest({
      ...base,
      runtimeBindingToken: signedRuntimeBindingToken(base, RuntimePodUid),
    });
    const attempts: string[] = [];
    const logs: unknown[] = [];
    const pool = new RecordingPlatformCredentialPool(["pfk_1", "pfk_2"]);
    const service = createService(new RecordingAuthenticator(), true, { verify: () => true }, {
      logger: { info: (record) => logs.push(record), error: (record) => logs.push(record) },
      credentialResolver: platformCredentialResolver(pool),
      providerStreamer: {
        stream: async function* (input) {
          attempts.push(input.credential?.source === "platform" ? input.credential.platformKey.keyId : "missing");
          yield textEvent(request, ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START, "");
          throw new ProviderKeyFailureError(retryableProviderFailure());
        },
      },
    });

    const events = await collectEvents(service.streamProviderRequest(request, metadata()));

    expect(attempts).toEqual(["pfk_1"]);
    expect(pool.recordedFailures).toEqual([]);
    expect(events).toHaveLength(2);
    expect(events[0]?.type).toBe(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START);
    expect(events[1]?.providerError?.error).toMatchObject({
      code: "provider_stream_error",
      retryable: true,
    });
  });

  test("discards open provider fragments before post-first-byte provider errors", async () => {
    const base = validProviderRequest({ model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "" } });
    const request = validProviderRequest({
      ...base,
      runtimeBindingToken: signedRuntimeBindingToken(base, RuntimePodUid),
    });
    const service = createService(new RecordingAuthenticator(), true, { verify: () => true }, {
      credentialResolver: platformCredentialResolver(new RecordingPlatformCredentialPool(["pfk_1"])),
      providerStreamer: {
        stream: async function* () {
          yield textEvent(request, ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START, "");
          yield reasoningEvent(request, ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_START, "");
          yield toolInputEvent(request, ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_START, "lookup", "");
          throw new ProviderKeyFailureError(retryableProviderFailure());
        },
      },
    });

    const events = await collectEvents(service.streamProviderRequest(request, metadata()));

    expect(events.map((event) => event.type)).toEqual([
      ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START,
      ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_START,
      ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_START,
      ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR,
    ]);
    expect(events[3]?.providerError?.error).toMatchObject({
      code: "provider_stream_error",
      retryable: true,
    });
  });

  test("nominal provider termination rejects unresolved fragment lifecycles without synthetic ends", async () => {
    const base = validProviderRequest({ model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "" } });
    const request = validProviderRequest({
      ...base,
      runtimeBindingToken: signedRuntimeBindingToken(base, RuntimePodUid),
    });
    const cases = [
      {
        name: "open text at finish",
        events: [textEvent(request, ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START, ""), finishEvent(request)],
        category: "finish",
        counts: { text: 1, reasoning: 0, toolInput: 0 },
      },
      {
        name: "ended tool input without call",
        events: [
          toolInputEvent(request, ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_START, "lookup", ""),
          toolInputEvent(request, ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_END, "lookup", ""),
          finishEvent(request),
        ],
        category: "finish",
        counts: { text: 0, reasoning: 0, toolInput: 1 },
      },
    ] as const;

    for (const testCase of cases) {
      const logs: unknown[] = [];
      const service = createService(new RecordingAuthenticator(), true, { verify: () => true }, {
        logger: { info: (record) => logs.push(record), error: (record) => logs.push(record) },
        providerStreamer: { stream: async function* () { yield* testCase.events; } },
      });

      const events = await collectEvents(service.streamProviderRequest(request, metadata()));
      expect(events.at(-1)?.providerError?.error, testCase.name).toMatchObject({
        code: "provider_stream_error",
        retryable: true,
        fatal: false,
      });
      expect(events.some((event) => event.type === ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH), testCase.name).toBe(false);
      expect(logs).toContainEqual(expect.objectContaining({
        event: "provider_stream_incomplete",
        "stream.terminal_category": testCase.category,
        "stream.open_text_count": testCase.counts.text,
        "stream.open_reasoning_count": testCase.counts.reasoning,
        "stream.open_tool_input_count": testCase.counts.toolInput,
      }));
    }
  });

  test("rejects a streamed tool call before its explicit input end", async () => {
    const base = validProviderRequest({ model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "" } });
    const request = validProviderRequest({
      ...base,
      runtimeBindingToken: signedRuntimeBindingToken(base, RuntimePodUid),
    });
    const logs: unknown[] = [];
    const service = createService(new RecordingAuthenticator(), true, { verify: () => true }, {
      logger: { info: (record) => logs.push(record), error: (record) => logs.push(record) },
      providerStreamer: {
        stream: async function* () {
          yield toolInputEvent(request, ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_START, "lookup", "", "tool_early");
          yield toolCallEvent(request, "tool_early", "lookup", '{"query":"hello"}');
        },
      },
    });

    const events = await collectEvents(service.streamProviderRequest(request, metadata()));

    expect(events.map((event) => event.type)).toEqual([
      ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_START,
      ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR,
    ]);
    expect(events.at(-1)?.providerError?.error).toMatchObject({
      code: "provider_stream_error",
      retryable: true,
      fatal: false,
    });
    expect(logs).toContainEqual(expect.objectContaining({
      event: "provider_stream_incomplete",
      "stream.terminal_category": "tool_call",
      "stream.open_tool_input_count": 1,
    }));
  });

  test("keeps concurrent streamed tool inputs isolated until each matching call consumes it", async () => {
    const base = validProviderRequest({ model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "" } });
    const request = validProviderRequest({
      ...base,
      runtimeBindingToken: signedRuntimeBindingToken(base, RuntimePodUid),
    });
    const streamEvents = [
      toolInputEvent(request, ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_START, "lookup_a", "", "tool_a"),
      toolInputEvent(request, ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_START, "lookup_b", "", "tool_b"),
      toolInputEvent(request, ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_DELTA, "lookup_b", '{"b":1}', "tool_b"),
      toolInputEvent(request, ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_END, "lookup_b", "", "tool_b"),
      toolCallEvent(request, "tool_b", "lookup_b", '{"b":1}'),
      toolInputEvent(request, ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_DELTA, "lookup_a", '{"a":1}', "tool_a"),
      toolInputEvent(request, ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_END, "lookup_a", "", "tool_a"),
      toolCallEvent(request, "tool_a", "lookup_a", '{"a":1}'),
      finishEvent(request),
    ];
    const service = createService(new RecordingAuthenticator(), true, { verify: () => true }, {
      providerStreamer: { stream: async function* () { yield* streamEvents; } },
    });

    const events = await collectEvents(service.streamProviderRequest(request, metadata()));

    expect(events).toEqual(streamEvents);
    expect(events.at(-1)?.type).toBe(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH);
  });

  test("platform fail-fast classification returns immediately without switching keys", async () => {
    const base = validProviderRequest({ model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "" } });
    const request = validProviderRequest({
      ...base,
      runtimeBindingToken: signedRuntimeBindingToken(base, RuntimePodUid),
    });
    const attempts: string[] = [];
    const logs: unknown[] = [];
    const pool = new RecordingPlatformCredentialPool(["pfk_1", "pfk_2"]);
    const service = createService(new RecordingAuthenticator(), true, { verify: () => true }, {
      logger: { info: (record) => logs.push(record), error: (record) => logs.push(record) },
      credentialResolver: platformCredentialResolver(pool),
      providerStreamer: {
        stream: async function* (input) {
          attempts.push(input.credential?.source === "platform" ? input.credential.platformKey.keyId : "missing");
          throw new ProviderKeyFailureError({
            action: "fail-fast",
            providerError: {
              code: "provider_request_invalid",
              message: "Provider rejected the request shape.",
              retryable: false,
              fatal: true,
              statusCode: 422,
            },
          });
        },
      },
    });

    const events = await collectEvents(service.streamProviderRequest(request, metadata()));

    expect(attempts).toEqual(["pfk_1"]);
    expect(events).toHaveLength(1);
    expect(events[0]?.providerError?.error).toMatchObject({
      code: "provider_request_invalid",
      retryable: false,
      fatal: true,
      statusCode: 422,
    });
    expect(logs).toEqual([
      expect.objectContaining({
        event: "provider_request_streamed",
        "error.class": "provider_http_rejection",
        "error.code": "provider_request_invalid",
        "provider.status_code": 422,
      }),
    ]);
  });

  test("non-catalog models fail closed before credential resolution", async () => {
    const unsupportedModels = [
      { providerId: "openai", modelId: "gpt-4.1", variant: "" },
      { providerId: "deepseek", modelId: "deepseek-chat", variant: "" },
    ];
    let credentialResolveCalls = 0;
    let streamCalls = 0;
    const service = createService(new RecordingAuthenticator(), true, { verify: () => true }, {
      credentialResolver: {
        resolve: async () => {
          credentialResolveCalls += 1;
          throw new Error("credential resolution must not run for non-catalog models");
        },
        recordPlatformFailure: () => undefined,
      } as unknown as ProviderCredentialResolver,
      providerStreamer: {
        stream: async function* () {
          streamCalls += 1;
          throw new Error("provider stream must not start for non-catalog models");
        },
      },
    });

    for (const model of unsupportedModels) {
      const base = validProviderRequest({ model });
      const request = validProviderRequest({
        ...base,
        runtimeBindingToken: signedRuntimeBindingToken(base, RuntimePodUid),
      });
      const events = await collectEvents(service.streamProviderRequest(request, metadata()));

      expect(events).toHaveLength(1);
      expect(events[0]?.providerError?.error).toMatchObject({
        code: "provider_unavailable",
        message: "Provider model is not approved by the Gateway catalog.",
        retryable: false,
        fatal: true,
      });
    }
    expect(credentialResolveCalls).toBe(0);
    expect(streamCalls).toBe(0);
  });

  test("current-stage non-empty ModelRef.variant is rejected at the Gateway boundary", async () => {
    let credentialResolveCalls = 0;
    let streamCalls = 0;
    const service = createService(new RecordingAuthenticator(), true, {
      verify: () => {
        throw new Error("binding token verification must not run for invalid current-stage variant");
      },
    }, {
      credentialResolver: {
        resolve: async () => {
          credentialResolveCalls += 1;
          throw new Error("credential resolution must not run for invalid current-stage variant");
        },
        recordPlatformFailure: () => undefined,
      } as unknown as ProviderCredentialResolver,
      providerStreamer: {
        stream: async function* () {
          streamCalls += 1;
          throw new Error("provider stream must not start for invalid current-stage variant");
        },
      },
    });
    const request = validProviderRequest({
      model: { providerId: "openai", modelId: "gpt-5.5", variant: "xhigh" },
    });

    await expectGrpcCode(collectEvents(service.streamProviderRequest(request, metadata())), status.INVALID_ARGUMENT);

    expect(credentialResolveCalls).toBe(0);
    expect(streamCalls).toBe(0);
  });

  test("session credential provider failures surface directly without platform key switching", async () => {
    const base = validProviderRequest({ model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "" } });
    const request = validProviderRequest({
      ...base,
      runtimeBindingToken: signedRuntimeBindingToken(base, RuntimePodUid),
    });
    let attempts = 0;
    const service = createService(new RecordingAuthenticator(), true, { verify: () => true }, {
      credentialResolver: sessionCredentialResolver(),
      providerStreamer: {
        stream: async function* () {
          attempts += 1;
          throw new ProviderKeyFailureError({
            action: "quarantine",
            providerError: {
              code: "provider_key_unavailable",
              message: "Provider key is not usable.",
              retryable: false,
              fatal: true,
              statusCode: 401,
            },
          });
        },
      },
    });

    const events = await collectEvents(service.streamProviderRequest(request, metadata()));

    expect(attempts).toBe(1);
    expect(events).toHaveLength(1);
    expect(events[0]?.providerError?.error).toMatchObject({
      code: "provider_key_unavailable",
      retryable: false,
      fatal: true,
      statusCode: 401,
    });
  });

  test("rejects runtime binding tokens that do not match request fields and caller pod UID", async () => {
    const request = validProviderRequest();
    const service = createService(
      new RecordingAuthenticator(),
      true,
      createRuntimeBindingTokenVerifier({
        hmacKey: BindingTokenKey,
        now: () => new Date("2026-01-01T00:00:00Z"),
      }),
    );

    await expectGrpcCode(collectEvents(service.streamProviderRequest({
      ...request,
      runtimeBindingToken: signedRuntimeBindingToken({ ...request, sessionId: "sesn_other" }, RuntimePodUid),
    }, metadata())), status.PERMISSION_DENIED);
    await expectGrpcCode(collectEvents(service.streamProviderRequest({
      ...request,
      runtimeBindingToken: signedRuntimeBindingToken(request, "pod_uid_other"),
    }, metadata())), status.PERMISSION_DENIED);
    await expectGrpcCode(collectEvents(service.streamProviderRequest({
      ...request,
      runtimeBindingToken: signedRuntimeBindingToken(request, RuntimePodUid, "2025-12-31T23:59:59Z"),
    }, metadata())), status.PERMISSION_DENIED);
  });

  test("RunWeb on the provider port is unimplemented and never executes web work", async () => {
    const request = validRunWebRequest();
    const service = createService(
      new RecordingAuthenticator(),
      true,
      createRuntimeBindingTokenVerifier({
        hmacKey: BindingTokenKey,
        now: () => new Date("2026-01-01T00:00:00Z"),
      }),
    );

    await expectGrpcCode(service.runWeb({
      ...request,
      runtimeBindingToken: signedRuntimeBindingToken(request, RuntimePodUid),
    }, metadata()), status.UNIMPLEMENTED);
  });

  test("resolves transient attachments before provider streaming and passes bytes once", async () => {
    const attachment = validProviderAttachment();
    const base = validProviderRequest({
      model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "" },
      attachments: [attachment],
    });
    const request = validProviderRequest({
      ...base,
      runtimeBindingToken: signedRuntimeBindingToken(base, RuntimePodUid),
    });
    const resolverCalls: unknown[] = [];
    const streamInputs: unknown[] = [];
    const attachmentResolver: ProviderAttachmentResolver = {
      resolve: async (input) => {
        resolverCalls.push(input);
        return {
          ok: true,
          attachments: [{
            ...attachment,
            data: new Uint8Array([1, 2, 3]),
          }],
          rejections: [],
        };
      },
    };
    const service = createService(new RecordingAuthenticator(), true, { verify: () => true }, {
      attachmentResolver,
      providerStreamer: {
        stream: async function* (input) {
          streamInputs.push(input);
          yield providerError(request, "provider_test_done", false);
        },
      },
    });

    const events = await collectEvents(service.streamProviderRequest(request, metadata()));

    expect(events).toHaveLength(1);
    expect(resolverCalls).toHaveLength(1);
    expect(resolverCalls[0]).toMatchObject({ runtimePodUid: RuntimePodUid });
    expect(streamInputs).toHaveLength(1);
    expect(streamInputs[0]).toMatchObject({
      resolvedAttachments: [{
        transient: { attachmentRef: "att_1" },
        fileBacked: undefined,
        data: new Uint8Array([1, 2, 3]),
      }],
    });
  });

  test("reports rejected attachment origins once before streaming the surviving subset", async () => {
    const transient = validProviderAttachment();
    const deleted = validFileBackedProviderAttachment({
      fileBacked: { sourceEventId: "sevt_user_deleted", fileId: "file_deleted" },
    });
    const base = validProviderRequest({
      model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "" },
      attachments: [transient, deleted],
    });
    const request = validProviderRequest({
      ...base,
      runtimeBindingToken: signedRuntimeBindingToken(base, RuntimePodUid),
    });
    const streamInputs: unknown[] = [];
    const service = createService(new RecordingAuthenticator(), true, { verify: () => true }, {
      attachmentResolver: {
        resolve: async () => ({
          ok: true,
          attachments: [{ ...transient, data: new Uint8Array([1, 2, 3]) }],
          rejections: [{
            transient: undefined,
            fileBacked: deleted.fileBacked,
            reason: ProviderAttachmentRejectionReason.PROVIDER_ATTACHMENT_REJECTION_REASON_DELETED,
          }],
        }),
      },
      providerStreamer: {
        stream: async function* (input) {
          streamInputs.push(input);
          yield finishEvent(request);
        },
      },
    });

    const events = await collectEvents(service.streamProviderRequest(request, metadata()));

    expect(events.map((event) => event.type)).toEqual([
      ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_ATTACHMENT_REJECTIONS,
      ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
    ]);
    expect(events[0]?.attachmentRejections?.rejections).toEqual([{
      transient: undefined,
      fileBacked: deleted.fileBacked,
      reason: ProviderAttachmentRejectionReason.PROVIDER_ATTACHMENT_REJECTION_REASON_DELETED,
    }]);
    expect(streamInputs).toEqual([
      expect.objectContaining({
        request: expect.objectContaining({ attachments: [transient] }),
        resolvedAttachments: [{ ...transient, data: new Uint8Array([1, 2, 3]) }],
      }),
    ]);
  });

  test("turns an internal attachment resolver RPC failure into one fatal whole-request error", async () => {
    const attachment = validFileBackedProviderAttachment();
    const base = validProviderRequest({
      model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "" },
      attachments: [attachment],
    });
    const request = validProviderRequest({
      ...base,
      runtimeBindingToken: signedRuntimeBindingToken(base, RuntimePodUid),
    });
    let providerStreamCalls = 0;
    const attachmentResolver = new BridgeAPIAttachmentResolver({
      address: "bridge.test:9090",
      tokenPath: "/var/run/bridge/token",
      metadataFactory: async () => new Metadata(),
      client: {
        resolveTransientAttachment: () => {
          throw new Error("unexpected transient attachment resolution");
        },
        resolveFileAttachmentMetadata: (_request, _metadata, callback) => {
          callback(Object.assign(new Error("bridge internal"), {
            code: status.INTERNAL,
            details: "bridge internal",
            metadata: new Metadata(),
          }), {
            attachments: [],
          });
          return {};
        },
      },
    });
    const service = createService(new RecordingAuthenticator(), true, { verify: () => true }, {
      attachmentResolver,
      providerStreamer: {
        stream: async function* () {
          providerStreamCalls += 1;
          yield finishEvent(request);
        },
      },
    });

    const events = await collectEvents(service.streamProviderRequest(request, metadata()));

    expect(providerStreamCalls).toBe(0);
    expect(events).toHaveLength(1);
    expect(events[0]?.type).toBe(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR);
    expect(events[0]?.attachmentRejections).toBeUndefined();
    expect(events[0]?.providerError?.error).toMatchObject({
      code: "provider_stream_error",
      retryable: false,
      fatal: true,
      statusCode: 500,
    });
  });

  test("attachment requests fail loudly without resolving credentials when resolver is missing", async () => {
    const base = validProviderRequest({
      model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "" },
      attachments: [validProviderAttachment()],
    });
    const request = validProviderRequest({
      ...base,
      runtimeBindingToken: signedRuntimeBindingToken(base, RuntimePodUid),
    });
    let credentialCalls = 0;
    const credentialResolver = {
      resolve: async () => {
        credentialCalls += 1;
        return { ok: true, credential: undefined };
      },
      recordPlatformFailure: () => undefined,
    } as unknown as ProviderCredentialResolver;
    const service = createService(new RecordingAuthenticator(), true, { verify: () => true }, {
      credentialResolver,
      providerStreamer: {
        stream: async function* () {
          throw new Error("provider streamer must not run without attachment bytes");
        },
      },
    });

    const events = await collectEvents(service.streamProviderRequest(request, metadata()));

    expect(events).toHaveLength(1);
    expect(events[0]?.providerError?.error).toMatchObject({
      code: "attachment_unavailable",
      retryable: true,
      fatal: false,
    });
    expect(credentialCalls).toBe(0);
  });

  test("overall timeout includes attachment resolution", async () => {
    const logs: unknown[] = [];
    const attachment = validProviderAttachment();
    const base = validProviderRequest({
      model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "" },
      attachments: [attachment],
      limits: { maxOutputTokens: 1024, timeoutMs: 5 },
    });
    const request = validProviderRequest({
      ...base,
      runtimeBindingToken: signedRuntimeBindingToken(base, RuntimePodUid),
    });
    const service = createService(new RecordingAuthenticator(), true, { verify: () => true }, {
      logger: { info: (record) => logs.push(record), error: (record) => logs.push(record) },
      attachmentResolver: { resolve: async () => never() },
    });

    const events = await collectEvents(service.streamProviderRequest(request, metadata()));

    expect(events).toHaveLength(1);
    expect(events[0]?.providerError?.error).toMatchObject({ code: "provider_timeout", statusCode: 504 });
    expect(logs.filter((record) => (record as { readonly event?: unknown }).event === "gateway_provider_timeout")).toEqual([
      expect.objectContaining({ "timeout.kind": "overall", "timeout.elapsed_ms": expect.any(Number) }),
    ]);
  });

  test("overall timeout includes credential resolution", async () => {
    const base = validProviderRequest({
      model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "" },
      limits: { maxOutputTokens: 1024, timeoutMs: 5 },
    });
    const request = validProviderRequest({
      ...base,
      runtimeBindingToken: signedRuntimeBindingToken(base, RuntimePodUid),
    });
    const service = createService(new RecordingAuthenticator(), true, { verify: () => true }, {
      credentialResolver: {
        resolve: async () => never(),
        recordPlatformFailure: () => undefined,
      } as unknown as ProviderCredentialResolver,
    });

    const events = await collectEvents(service.streamProviderRequest(request, metadata()));

    expect(events).toHaveLength(1);
    expect(events[0]?.providerError?.error).toMatchObject({ code: "provider_timeout", statusCode: 504 });
  });

  test("not-ready Gateway rejects valid provider requests before provider shell work", async () => {
    const logs: unknown[] = [];
    const service = createService(new RecordingAuthenticator(), false, { verify: () => true }, {
      logger: { info: (record) => logs.push(record), error: (record) => logs.push(record) },
    });

    await expectGrpcCode(collectEvents(service.streamProviderRequest(validProviderRequest(), metadata())), status.UNAVAILABLE);
    expect(logs).toEqual([
      expect.objectContaining({
        event: "provider_request_streamed",
        "request.outcome": "failed",
        "error.class": "grpc_status",
        "error.code": "14",
      }),
    ]);
  });
});

function createService(
  authenticator: GatewayAuthenticator,
  ready = true,
  runtimeBindingTokenVerifier: RuntimeBindingTokenVerifier = { verify: () => true },
  overrides: {
    readonly maxConcurrentTurns?: number | undefined;
    readonly credentialResolver?: ProviderCredentialResolver | undefined;
    readonly attachmentResolver?: ProviderAttachmentResolver | undefined;
    readonly providerStreamer?: ProviderRequestStreamer | undefined;
    readonly providerStreamTimeouts?: { readonly firstByteTimeoutMs?: number | undefined; readonly interChunkTimeoutMs?: number | undefined } | undefined;
    readonly logger?: GatewayLogger | undefined;
  } = {},
): ProviderGatewayServiceShell {
  const { logger, ...shellOverrides } = overrides;
  return new ProviderGatewayServiceShell({
    authenticator,
    runtimeBindingTokenVerifier,
    ready: () => ready,
    logger: logger ?? { info: () => undefined, error: () => undefined },
    ...shellOverrides,
  });
}


function platformCredentialResolver(pool: PlatformCredentialPool): ProviderCredentialResolver {
  return new ProviderCredentialResolver({
    store: EmptyCredentialStore,
    platformPool: pool,
    masterKeyHex: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  });
}

function sessionCredentialResolver(): ProviderCredentialResolver {
  return {
    resolve: async () => ({
      ok: true,
      credential: {
        source: "session",
        authType: "provider_api_key",
        providerId: "anthropic",
        supplyMode: "anthropic-api-key",
        vaultId: "vlt_1",
        credentialId: "cred_1",
        accessMode: "api_key",
        apiKey: "sk-session-anthropic",
      },
    }),
    recordPlatformFailure: () => undefined,
  } as unknown as ProviderCredentialResolver;
}

function platformOpenAICredentialResolver(onFailure: () => void): ProviderCredentialResolver {
  return {
    resolve: async () => ({
      ok: true,
      credential: {
        source: "platform",
        authType: "provider_api_key",
        providerId: "openai",
        supplyMode: "openai-api-key",
        platformKey: {
          keyId: "pfk_openai_local_capability",
          providerId: "openai",
          key: "sk-platform-openai",
          weight: 1,
          priority: 0,
          cacheScope: "openai-local-capability",
        },
      },
    }),
    recordPlatformFailure: onFailure,
  } as unknown as ProviderCredentialResolver;
}

async function productionCredentialResolver(
  pool: PlatformCredentialPool,
  withSessionCredential: boolean,
): Promise<ProviderCredentialResolver> {
  const encryptedAuth = await encryptAES256GCM(
    new TextEncoder().encode(JSON.stringify({
      type: "provider_api_key",
      provider_id: "openai",
      access_mode: "user_api_key",
      token: "sk-session-openai",
    })),
    CredentialMasterKeyHex,
    () => new Uint8Array(12).fill(7),
  );
  const store: GatewayCredentialStore = {
    async loadActiveSessionProviderAuth() {
      return withSessionCredential
        ? [{
            providerId: "openai",
            vaultId: "vlt_service",
            credentialId: "cred_service",
            accessMode: "user_api_key",
            credentialAuthType: "provider_api_key",
            credentialProviderId: "openai",
            credentialAccessMode: "user_api_key",
            encryptedAuth,
            archived: false,
            revoked: false,
          }]
        : [];
    },
    async loadPlatformProviderKeyRows() {
      return [];
    },
  };
  return new ProviderCredentialResolver({
    store,
    platformPool: pool,
    masterKeyHex: CredentialMasterKeyHex,
  });
}

function metadata(): Metadata {
  const value = new Metadata();
  value.set("authorization", "bearer request-token");
  return value;
}

async function expectGrpcCode(promise: Promise<unknown>, code: status): Promise<void> {
  try {
    await promise;
    throw new Error(`expected ${code}`);
  } catch (error) {
    expect(error).toBeInstanceOf(GrpcStatusError);
    expect((error as GrpcStatusError).code).toBe(code);
  }
}

async function collectEvents(events: AsyncIterable<ProviderStreamEvent>): Promise<readonly ProviderStreamEvent[]> {
  const out: ProviderStreamEvent[] = [];
  for await (const event of events) {
    out.push(event);
  }
  return out;
}

function textEvent(
  request: { readonly requestId: string; readonly modelRequestId: string },
  type: ProviderStreamEventType,
  text: string,
): ProviderStreamEvent {
  return {
    requestId: request.requestId,
    modelRequestId: request.modelRequestId,
    type,
    text: {
      id: "text_1",
      text,
      metadataJson: "{}",
    },
  };
}

function reasoningEvent(
  request: { readonly requestId: string; readonly modelRequestId: string },
  type: ProviderStreamEventType,
  text: string,
): ProviderStreamEvent {
  return {
    requestId: request.requestId,
    modelRequestId: request.modelRequestId,
    type,
    reasoning: {
      id: "reasoning_1",
      text,
      metadataJson: "{}",
    },
  };
}

function toolInputEvent(
  request: { readonly requestId: string; readonly modelRequestId: string },
  type: ProviderStreamEventType,
  name: string,
  text: string,
  id = "tool_1",
): ProviderStreamEvent {
  return {
    requestId: request.requestId,
    modelRequestId: request.modelRequestId,
    type,
    toolInput: {
      id,
      name,
      text,
      metadataJson: "{}",
    },
  };
}

function toolCallEvent(
  request: { readonly requestId: string; readonly modelRequestId: string },
  id: string,
  name: string,
  inputJson: string,
): ProviderStreamEvent {
  return {
    requestId: request.requestId,
    modelRequestId: request.modelRequestId,
    type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_CALL,
    toolCall: { id, name, inputJson, metadataJson: "{}" },
  };
}

function providerError(request: RuntimeBindingRequestIdentity & { readonly requestId: string; readonly modelRequestId: string }, code: string, retryable: boolean): ProviderStreamEvent {
  return {
    requestId: request.requestId,
    modelRequestId: request.modelRequestId,
    type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR,
    providerError: {
      metadataJson: "{}",
      error: {
        code,
        message: "Provider unavailable.",
        retryable,
        fatal: !retryable,
        statusCode: 503,
        retryAfterMs: 0,
      },
    },
  };
}

function finishEvent(request: { readonly requestId: string; readonly modelRequestId: string }): ProviderStreamEvent {
  return {
    requestId: request.requestId,
    modelRequestId: request.modelRequestId,
    type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
    finish: {
      reason: ProviderFinishReason.PROVIDER_FINISH_REASON_STOP,
      usage: {
        inputTotalTokens: 1,
        inputUncachedTokens: 1,
        outputTotalTokens: 1,
        totalTokens: 2,
        providerUsageJson: "{}",
      },
      metadataJson: "{}",
    },
  };
}

function never(): Promise<never> {
  return new Promise<never>(() => undefined);
}

class RecordingAuthenticator implements GatewayAuthenticator {
  readonly calls: string[] = [];

  constructor(
    private readonly result: Awaited<ReturnType<GatewayAuthenticator["authenticate"]>> = {
      ok: true,
      serviceAccount: { namespace: "tetral-agent-runtime", name: "agent-runtime", podUid: RuntimePodUid },
    },
  ) {}

  async authenticate(input: Parameters<GatewayAuthenticator["authenticate"]>[0]) {
    this.calls.push(input.method);
    return this.result;
  }
}

class RecordingPlatformCredentialPool implements PlatformCredentialPool {
  readonly recordedFailures: Array<{ readonly keyId: string; readonly classification: ProviderFailureClassification }> = [];
  selectCalls = 0;

  constructor(private readonly keyIds: readonly string[]) {}

  async select(providerId: PlatformHostedProviderId, options: PlatformKeySelectionOptions = {}) {
    this.selectCalls += 1;
    const keyId = this.keyIds.find((candidate) => !options.excludeKeyIds?.has(candidate));
    if (keyId === undefined) {
      return {
        ok: false as const,
        error: {
          code: "platform_keys_exhausted",
          message: "Platform provider keys are temporarily exhausted.",
          retryable: true,
          fatal: false,
          statusCode: 503,
          retryAfterMs: 5_000,
        },
      };
    }
    return {
      ok: true as const,
      key: {
        keyId,
        providerId,
        key: `sk-${keyId}`,
        weight: 1,
        priority: 0,
        cacheScope: `${providerId}-scope`,
      },
    };
  }

  recordFailure(keyId: string, classification: ProviderFailureClassification): void {
    this.recordedFailures.push({ keyId, classification });
  }
}

const EmptyCredentialStore: GatewayCredentialStore = {
  async loadActiveSessionProviderAuth() {
    return [];
  },
  async loadPlatformProviderKeyRows(): Promise<readonly EncryptedPlatformProviderKeyRow[]> {
    return [];
  },
};

function retryableProviderFailure(): ProviderFailureClassification {
  return {
    action: "retryable",
    providerError: {
      code: "provider_stream_error",
      message: "Provider stream failed.",
      retryable: true,
      fatal: false,
      statusCode: 502,
    },
  };
}

function signedRuntimeBindingToken(request: RuntimeBindingRequestIdentity, runtimePodUid: string, expiresAt = "2026-01-01T00:05:00Z"): string {
  const payload = JSON.stringify({
    v: 1,
    workspace_id: request.workspaceId,
    session_id: request.sessionId,
    session_thread_id: request.sessionThreadId,
    binding_id: request.bindingId,
    binding_generation: request.bindingGeneration,
    runtime_pod_uid: runtimePodUid,
    exp: Math.floor(new Date(expiresAt).getTime() / 1000),
  });
  const payloadPart = Buffer.from(payload, "utf8").toString("base64url");
  const signaturePart = createHmac("sha256", BindingTokenKey).update(payloadPart).digest("base64url");
  return `rtbt_v1.${payloadPart}.${signaturePart}`;
}
