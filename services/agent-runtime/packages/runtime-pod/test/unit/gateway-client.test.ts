import { describe, expect, test } from "bun:test";
import { Metadata, Server, ServerCredentials, status } from "@grpc/grpc-js";
import { Readable } from "node:stream";
import {
  ProviderRequestKind,
  ProviderGatewayServiceService,
  ProviderStreamEventType,
  ProviderContextRole,
  SystemCacheHint,
  SystemSegmentKind,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type {
  ProviderGatewayServiceClient,
  ProviderGatewayServiceServer,
  ProviderRequest,
  ProviderStreamEvent,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { Effect, Stream } from "effect";
import { createLLMService } from "@tetral/agent-runtime-core/src/llm/llm-service.js";
import { MaxGatewayRequestGrpcMessageBytes, MaxGatewayStreamEventGrpcMessageBytes } from "../../src/bounds.js";
import { RuntimeGatewayTransportCompletionAllowanceMs, RuntimeGatewayTransportDeadlineGuardMs, RuntimePodGatewayClient, runtimeProviderStreamObserver } from "../../src/gateway-client.js";

describe("Runtime Pod Gateway client", () => {
  test("logs one bounded open record after the generated call handle exists", async () => {
    const records: unknown[] = [];
    const request = providerRequest();
    const error = await collectGatewayError(new RuntimePodGatewayClient({
      address: "gateway.test:9090",
      tokenPath: "/var/run/token",
      client: failingGatewayClient(status.INVALID_ARGUMENT, "secret raw rejection"),
      metadataFactory: async () => new Metadata(),
      logger: { info: (record) => records.push(record), error: (record) => records.push(record) },
    }), request);

    expect(error).toMatchObject({ code: "gateway_protocol_error", fatal: true });
    expect(records).toEqual([expect.objectContaining({
      event: "runtime_provider_stream_opened",
      "workspace.id": request.workspaceId,
      "session.id": request.sessionId,
      "thread.id": request.sessionThreadId,
      "request.id": request.requestId,
      "model_request.id": request.modelRequestId,
    })]);
    expect(JSON.stringify(records)).not.toContain("secret raw rejection");
  });

  test("does not duplicate transport-failure logging after stream progress", async () => {
    const records: unknown[] = [];
    const request = providerRequest();
    const error = await collectGatewayError(new RuntimePodGatewayClient({
      address: "gateway.test:9090",
      tokenPath: "/var/run/token",
      client: eventThenFailingGatewayClient(status.UNAVAILABLE, "secret post-stream rejection"),
      metadataFactory: async () => new Metadata(),
      logger: { info: (record) => records.push(record), error: (record) => records.push(record) },
    }), request);

    expect(error).toMatchObject({ code: "gateway_unavailable", retryable: true, fatal: false });
    expect(records).toEqual([expect.objectContaining({
      event: "runtime_provider_stream_opened",
      "request.id": request.requestId,
      "model_request.id": request.modelRequestId,
    })]);
    expect(JSON.stringify(records)).not.toContain("secret post-stream rejection");
  });

  test("classifies the closed set of remote gRPC statuses", async () => {
    for (const scenario of [
      { code: status.INVALID_ARGUMENT, wantCode: "gateway_protocol_error", retryable: false, fatal: true },
      { code: status.UNAUTHENTICATED, wantCode: "gateway_stream_error", retryable: false, fatal: false },
      { code: status.PERMISSION_DENIED, wantCode: "gateway_stream_error", retryable: false, fatal: false },
      { code: status.UNIMPLEMENTED, wantCode: "gateway_stream_error", retryable: false, fatal: false },
      { code: status.NOT_FOUND, wantCode: "gateway_stream_error", retryable: false, fatal: false },
      { code: status.INTERNAL, wantCode: "gateway_stream_error", retryable: false, fatal: true },
      { code: status.UNAVAILABLE, wantCode: "gateway_unavailable", retryable: true, fatal: false },
      { code: status.DEADLINE_EXCEEDED, wantCode: "gateway_unavailable", retryable: true, fatal: false },
      { code: status.RESOURCE_EXHAUSTED, wantCode: "gateway_unavailable", retryable: true, fatal: false },
      { code: status.UNKNOWN, wantCode: "gateway_stream_error", retryable: false, fatal: false },
    ] as const) {
      const error = await collectGatewayError(new RuntimePodGatewayClient({
        address: "gateway.test:9090",
        tokenPath: "/var/run/token",
        client: failingGatewayClient(scenario.code),
        metadataFactory: async () => new Metadata(),
      }), providerRequest());

      expect(error).toMatchObject({
        type: "gateway-client",
        code: scenario.wantCode,
        retryable: scenario.retryable,
        fatal: scenario.fatal,
        statusCode: scenario.code,
      });
    }
  });

  test("preserves an already classified abort before the first event", async () => {
    const records: unknown[] = [];
    const abortController = new AbortController();
    abortController.abort();
    const client = new RuntimePodGatewayClient({
      address: "gateway.test:9090",
      tokenPath: "/var/run/token",
      client: recordingGatewayClient(() => undefined),
      metadataFactory: async () => new Metadata(),
      logger: { info: (record) => records.push(record), error: (record) => records.push(record) },
    });

    const handle = await client.streamProviderRequest(providerRequest(), {
      abortSignal: abortController.signal,
    });
    await Effect.runPromise(Stream.runCollect(handle.events));

    expect(await handle.completion).toEqual({ outcome: "cancelled", cancelKind: "caller" });
    expect(records).toEqual([expect.objectContaining({ event: "runtime_provider_stream_opened" })]);
  });

  test("rejects an oversized ProviderRequest before metadata or transport work", async () => {
    let metadataCalls = 0;
    let transportCalls = 0;
    const records: unknown[] = [];
    const request = providerRequest();
    request.context[0]!.content = [{
      text: { text: "x".repeat(MaxGatewayRequestGrpcMessageBytes) },
    }];
    const client = new RuntimePodGatewayClient({
      address: "gateway.test:9090",
      tokenPath: "/var/run/token",
      client: recordingGatewayClient(() => {
        transportCalls++;
      }),
      metadataFactory: async () => {
        metadataCalls++;
        return new Metadata();
      },
      logger: { info: (record) => records.push(record), error: (record) => records.push(record) },
    });

    const error = await collectGatewayError(client, request);

    expect(error).toMatchObject({
      code: "gateway_protocol_error",
      retryable: false,
      fatal: true,
    });
    expect(metadataCalls).toBe(0);
    expect(transportCalls).toBe(0);
    expect(records).toEqual([expect.objectContaining({
      event: "runtime_provider_stream_failed",
      "transport.stage": "request_fuse",
      "request.id": request.requestId,
      "model_request.id": request.modelRequestId,
      "error.code": "gateway_protocol_error",
    })]);
  });

  test("records safe identity telemetry for every failure before a stream handle exists", async () => {
    for (const scenario of [
      {
        stage: "metadata",
        client: recordingGatewayClient(() => undefined),
        metadataFactory: async (): Promise<Metadata> => { throw new Error("raw metadata credential"); },
      },
      {
        stage: "stream_call",
        client: { streamProviderRequest: () => { throw Object.assign(new Error("raw synchronous transport"), { code: status.UNAVAILABLE }); } } as unknown as ProviderGatewayServiceClient,
        metadataFactory: async () => new Metadata(),
      },
    ] as const) {
      const records: unknown[] = [];
      const request = providerRequest();
      await collectGatewayError(new RuntimePodGatewayClient({
        address: "gateway.test:9090",
        tokenPath: "/var/run/token",
        client: scenario.client,
        metadataFactory: scenario.metadataFactory,
        logger: { info: (record) => records.push(record), error: (record) => records.push(record) },
      }), request);

      expect(records).toEqual([expect.objectContaining({
        event: "runtime_provider_stream_failed",
        "transport.stage": scenario.stage,
        "workspace.id": request.workspaceId,
        "session.id": request.sessionId,
        "thread.id": request.sessionThreadId,
        "request.id": request.requestId,
        "model_request.id": request.modelRequestId,
      })]);
      expect(JSON.stringify(records)).not.toContain("raw metadata credential");
      expect(JSON.stringify(records)).not.toContain("raw synchronous transport");
    }
  });

  test("classifies local-send and Gateway-receive size failures precisely", async () => {
    const cases = [
      {
        details: "Attempted to send message with a size larger than 4194304",
        message: "Gateway request exceeded the local transport fuse.",
      },
      {
        details: "Received message larger than max (33554433 vs 33554432)",
        message: "Gateway rejected the request above its transport fuse.",
      },
      {
        details: "Received message that decompresses to a size larger than 33554432",
        message: "Gateway rejected the request above its transport fuse.",
      },
    ];
    for (const testCase of cases) {
      const error = await collectGatewayError(new RuntimePodGatewayClient({
        address: "gateway.test:9090",
        tokenPath: "/var/run/token",
        client: failingGatewayClient(status.RESOURCE_EXHAUSTED, testCase.details),
        metadataFactory: async () => new Metadata(),
      }), providerRequest());

      expect(error).toMatchObject({
        code: "gateway_protocol_error",
        message: testCase.message,
        retryable: false,
        fatal: true,
        statusCode: status.RESOURCE_EXHAUSTED,
      });
    }
  });

  test("classifies the local decompressed receive fuse as a fatal protocol error", async () => {
    const error = await collectGatewayError(new RuntimePodGatewayClient({
      address: "gateway.test:9090",
      tokenPath: "/var/run/token",
      client: failingGatewayClient(
        status.RESOURCE_EXHAUSTED,
        `Received message that decompresses to a size larger than ${MaxGatewayStreamEventGrpcMessageBytes}`,
      ),
      metadataFactory: async () => new Metadata(),
    }), providerRequest());

    expect(error).toMatchObject({
      code: "gateway_protocol_error",
      retryable: false,
      fatal: true,
      statusCode: status.RESOURCE_EXHAUSTED,
    });
  });

  test("anchors one call deadline to request timeout plus the transport allowance", async () => {
    let observedDeadline = 0;
    const request = providerRequest();
    request.limits = { maxOutputTokens: 128, timeoutMs: 42_000 };
    const client = new RuntimePodGatewayClient({
      address: "gateway.test:9090",
      tokenPath: "/var/run/token",
      nowEpochMs: () => 1_000,
      client: {
        streamProviderRequest(_request, _metadata, options) {
          observedDeadline = (options?.deadline as Date).getTime();
          return readableCall([]);
        },
      } as ProviderGatewayServiceClient,
      metadataFactory: async () => new Metadata(),
    });

    const handle = await client.streamProviderRequest(request);
    await Effect.runPromise(Stream.runCollect(handle.events));
    expect(await handle.completion).toEqual({ outcome: "eof" });
    expect(observedDeadline).toBe(1_000 + 42_000 + RuntimeGatewayTransportCompletionAllowanceMs + RuntimeGatewayTransportDeadlineGuardMs);
  });

  test("deducts Gateway call setup from the single call-start completion budget", async () => {
    let now = 1_000;
    let observedDeadline = 0;
    let scheduledDelay = -1;
    const waiting = new Readable({ objectMode: true, read() {} }) as Readable & { cancel: () => void };
    waiting.cancel = () => waiting.destroy();
    const request = providerRequest();
    request.limits = { maxOutputTokens: 128, timeoutMs: 42_000 };
    const client = new RuntimePodGatewayClient({
      address: "gateway.test:9090",
      tokenPath: "/var/run/token",
      nowEpochMs: () => now,
      scheduleTimeout(_callback, durationMs) {
        scheduledDelay = durationMs;
        return 1 as unknown as ReturnType<typeof setTimeout>;
      },
      cancelTimeout: () => undefined,
      client: {
        streamProviderRequest(_request, _metadata, options) {
          observedDeadline = (options?.deadline as Date).getTime();
          now += 4_000;
          return waiting;
        },
      } as ProviderGatewayServiceClient,
      metadataFactory: async () => new Metadata(),
    });

    const handle = await client.streamProviderRequest(request);

    expect(observedDeadline).toBe(1_000 + 42_000 + RuntimeGatewayTransportCompletionAllowanceMs + RuntimeGatewayTransportDeadlineGuardMs);
    expect(scheduledDelay).toBe(38_000 + RuntimeGatewayTransportCompletionAllowanceMs);
    handle.cancel("consumer_early_exit");
    expect(await handle.completion).toEqual({ outcome: "cancelled", cancelKind: "consumer_early_exit" });
  });

  test("actively cancels the reader and call when the completion deadline wins", async () => {
    let fireDeadline: (() => void) | undefined;
    let scheduledDelay = -1;
    let cancelCalls = 0;
    const waiting = new Readable({ objectMode: true, read() {} }) as Readable & { cancel: () => void };
    waiting.cancel = () => {
      cancelCalls += 1;
      waiting.destroy();
    };
    const request = providerRequest();
    request.limits = { maxOutputTokens: 128, timeoutMs: 42_000 };
    const client = new RuntimePodGatewayClient({
      address: "gateway.test:9090",
      tokenPath: "/var/run/token",
      nowEpochMs: () => 1_000,
      scheduleTimeout(callback, durationMs) {
        fireDeadline = callback;
        scheduledDelay = durationMs;
        return 1 as unknown as ReturnType<typeof setTimeout>;
      },
      cancelTimeout: () => undefined,
      client: { streamProviderRequest: () => waiting } as unknown as ProviderGatewayServiceClient,
      metadataFactory: async () => new Metadata(),
    });

    const handle = await client.streamProviderRequest(request);
    expect(scheduledDelay).toBe(42_000 + RuntimeGatewayTransportCompletionAllowanceMs);
    fireDeadline?.();
    expect(await handle.completion).toEqual({ outcome: "completion_deadline" });
    expect(cancelCalls).toBe(1);
  });

  test("consumer validation cancellation is typed and cancels the call once", async () => {
    let cancelCalls = 0;
    const waiting = new Readable({ objectMode: true, read() {} }) as Readable & { cancel: () => void };
    waiting.cancel = () => {
      cancelCalls += 1;
      waiting.destroy();
    };
    const client = new RuntimePodGatewayClient({
      address: "gateway.test:9090",
      tokenPath: "/var/run/token",
      client: { streamProviderRequest: () => waiting } as unknown as ProviderGatewayServiceClient,
      metadataFactory: async () => new Metadata(),
    });

    const handle = await client.streamProviderRequest(providerRequest());
    handle.cancel("consumer_validation");
    handle.cancel("consumer_early_exit");
    expect(await handle.completion).toEqual({ outcome: "cancelled", cancelKind: "consumer_validation" });
    expect(cancelCalls).toBe(1);
  });

  test("delivers a backpressured grpc-js burst through FINISH and normal EOF", async () => {
    const request = providerRequest();
    let crossedHighWaterMark = false;
    const implementation: ProviderGatewayServiceServer = {
      streamProviderRequest(call) {
        for (let index = 0; index < 17; index += 1) {
          const accepted = call.write({
            type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_DELTA,
            text: { id: "text_1", text: String(index), metadataJson: "" },
          });
          if (!accepted) crossedHighWaterMark = true;
        }
        call.write({
          type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
          finish: {
            reason: 1,
            usage: { inputTotalTokens: 1, inputUncachedTokens: 1, outputTotalTokens: 1, totalTokens: 2, providerUsageJson: "{}" },
            metadataJson: "{}",
            contextWindowTokens: 128_000,
            outputTokenLimit: 8_192,
          },
        });
        call.end();
      },
      runWeb(_call, callback) {
        callback(new Error("not implemented"));
      },
    };
    const server = new Server();
    server.addService(ProviderGatewayServiceService, implementation);
    const port = await new Promise<number>((resolve, reject) => {
      server.bindAsync("127.0.0.1:0", ServerCredentials.createInsecure(), (error, boundPort) => {
        if (error !== null) reject(error);
        else resolve(boundPort);
      });
    });
    const client = new RuntimePodGatewayClient({
      address: `127.0.0.1:${port}`,
      tokenPath: "/unused",
      metadataFactory: async () => new Metadata(),
    });

    try {
      const handle = await client.streamProviderRequest(request);
      const events: ProviderStreamEvent[] = [];
      for await (const providerEvent of Stream.toAsyncIterable(handle.events)) {
        events.push(providerEvent);
        if (events.length === 1) await Bun.sleep(20);
      }
      expect(crossedHighWaterMark).toBe(true);
      expect(events).toHaveLength(18);
      expect(events.at(-1)?.type).toBe(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH);
      expect(await handle.completion).toEqual({ outcome: "eof" });
    } finally {
      await new Promise<void>((resolve) => server.tryShutdown(() => resolve()));
    }
  });

  test("cancels generated grpc-js calls on downstream early exit", async () => {
    const request = providerRequest();
    let cancelledCalls = 0;
    const implementation: ProviderGatewayServiceServer = {
      streamProviderRequest(call) {
        call.on("cancelled", () => { cancelledCalls += 1; });
        call.write({
          type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START,
          text: { id: "text_1", text: "", metadataJson: "" },
        });
      },
      runWeb(_call, callback) {
        callback(new Error("not implemented"));
      },
    };
    const server = new Server();
    server.addService(ProviderGatewayServiceService, implementation);
    const port = await new Promise<number>((resolve, reject) => {
      server.bindAsync("127.0.0.1:0", ServerCredentials.createInsecure(), (error, boundPort) => {
        if (error !== null) reject(error);
        else resolve(boundPort);
      });
    });
    const client = new RuntimePodGatewayClient({
      address: `127.0.0.1:${port}`,
      tokenPath: "/unused",
      metadataFactory: async () => new Metadata(),
    });
    try {
      const stream = createLLMService(client).stream(request);
      const events = Array.from(await Effect.runPromise(Stream.runCollect(Stream.take(stream, 1))));
      expect(events).toHaveLength(1);
      for (let attempt = 0; attempt < 50 && cancelledCalls === 0; attempt += 1) {
        await Bun.sleep(2);
      }
      expect(cancelledCalls).toBe(1);
    } finally {
      server.forceShutdown();
    }
  });

  test("records one safe semantic close after terminal and EOF and remains fail-open", async () => {
    const records: unknown[] = [];
    const request = providerRequest();
    request.runtimeBindingToken = "CANARY_TOKEN_VALUE";
    request.context[0]!.content = [{ text: { text: "raw provider payload marker" } }];
    const finish: ProviderStreamEvent = {
      type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
      finish: {
        reason: 1,
        usage: { inputTotalTokens: 1, inputUncachedTokens: 1, outputTotalTokens: 1, totalTokens: 2, providerUsageJson: "{}" },
        metadataJson: "{}",
        contextWindowTokens: 128_000,
        outputTokenLimit: 8_192,
      },
    };
    const logger = { info: (record: unknown) => records.push(record), error: (record: unknown) => records.push(record) };
    const client = new RuntimePodGatewayClient({
      address: "gateway.test:9090",
      tokenPath: "/unused",
      client: { streamProviderRequest: () => readableCall([finish]) } as unknown as ProviderGatewayServiceClient,
      metadataFactory: async () => new Metadata(),
      logger,
    });

    const events = Array.from(await Effect.runPromise(Stream.runCollect(
      createLLMService(client, runtimeProviderStreamObserver(logger, () => 10)).stream(request),
    )));
    expect(events.at(-1)?.type).toBe("finish");
    expect(records.map((record) => (record as { event?: string }).event)).toEqual([
      "runtime_provider_stream_opened",
      "runtime_provider_terminal_observed",
      "runtime_provider_stream_closed",
    ]);
    expect(records[2]).toMatchObject({
      "transport.outcome": "eof",
      "terminal.candidate_observed": true,
    });
    expect(JSON.stringify(records)).not.toContain("CANARY_TOKEN_VALUE");
    expect(JSON.stringify(records)).not.toContain("raw provider payload marker");

    const throwingLogger = { info: () => { throw new Error("logger failed"); }, error: () => { throw new Error("logger failed"); } };
    const throwingClient = new RuntimePodGatewayClient({
      address: "gateway.test:9090",
      tokenPath: "/unused",
      client: { streamProviderRequest: () => readableCall([finish]) } as unknown as ProviderGatewayServiceClient,
      metadataFactory: async () => new Metadata(),
      logger: throwingLogger,
    });
    const failOpenEvents = await Effect.runPromise(Stream.runCollect(
      createLLMService(throwingClient, runtimeProviderStreamObserver(throwingLogger)).stream(request),
    ));
    expect(Array.from(failOpenEvents).at(-1)?.type).toBe("finish");
  });

  test("records a validated terminal candidate once when transport cleanup fails", async () => {
    const finish: ProviderStreamEvent = {
      type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
      finish: {
        reason: 1,
        usage: { inputTotalTokens: 1, inputUncachedTokens: 1, outputTotalTokens: 1, totalTokens: 2, providerUsageJson: "{}" },
        metadataJson: "{}",
        contextWindowTokens: 128_000,
        outputTokenLimit: 8_192,
      },
    };
    const request = providerRequest();
    request.runtimeBindingToken = "CANARY_TOKEN_VALUE";
    request.context[0]!.content = [{ text: { text: "raw provider payload marker" } }];

    const transportRecords: unknown[] = [];
    const transportLogger = { info: (record: unknown) => transportRecords.push(record), error: (record: unknown) => transportRecords.push(record) };
    const transportClient = new RuntimePodGatewayClient({
      address: "gateway.test:9090",
      tokenPath: "/unused",
      client: {
        streamProviderRequest() {
          return readableCall((async function* () {
            yield finish;
            throw Object.assign(new Error("raw-secret-body"), { code: status.UNAVAILABLE, details: "raw-secret-body" });
          })());
        },
      } as unknown as ProviderGatewayServiceClient,
      metadataFactory: async () => new Metadata(),
      logger: transportLogger,
    });
    const transportError = await Effect.runPromise(Stream.runCollect(
      createLLMService(transportClient, runtimeProviderStreamObserver(transportLogger)).stream(request),
    ).pipe(Effect.flip));
    expect(transportError.error).toMatchObject({ code: "gateway_stream_error", retryable: true, fatal: false });
    expect(transportRecords.map((record) => (record as { event?: string }).event)).toEqual([
      "runtime_provider_stream_opened",
      "runtime_provider_terminal_observed",
      "runtime_provider_stream_closed",
    ]);
    expect(transportRecords[2]).toMatchObject({
      "transport.outcome": "transport_failure",
      "terminal.candidate_observed": true,
    });
    expect(JSON.stringify(transportRecords)).not.toContain("CANARY_TOKEN_VALUE");
    expect(JSON.stringify(transportRecords)).not.toContain("raw provider payload marker");
    expect(JSON.stringify(transportRecords)).not.toContain("raw-secret-body");
  });
});

async function collectGatewayError(client: RuntimePodGatewayClient, request: ProviderRequest) {
  try {
    const handle = await client.streamProviderRequest(request);
    await Effect.runPromise(Stream.runCollect(handle.events));
    const completion = await handle.completion;
    if (completion.outcome === "transport_failure") {
      return completion.error;
    }
    if (completion.outcome === "cancelled") {
      return {
        type: "gateway-client" as const,
        code: "gateway_cancelled" as const,
        message: "Gateway provider stream was cancelled.",
        retryable: false,
        fatal: false,
      };
    }
    throw new Error(`expected transport failure, got ${completion.outcome}`);
  } catch (error) {
    return error;
  }
}

function failingGatewayClient(code: number, details = "gateway request failed"): ProviderGatewayServiceClient {
  return {
    streamProviderRequest() {
      return readableCall((async function* (): AsyncGenerator<ProviderStreamEvent> {
        throw Object.assign(new Error(details), { code, details });
      })());
    },
  } as unknown as ProviderGatewayServiceClient;
}

function eventThenFailingGatewayClient(code: number, details: string): ProviderGatewayServiceClient {
  return {
    streamProviderRequest(request: ProviderRequest) {
      return readableCall((async function* () {
          yield {
            type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START,
            text: { id: "text_1", text: "", metadataJson: "" },
          };
          throw Object.assign(new Error(details), { code, details });
      })());
    },
  } as unknown as ProviderGatewayServiceClient;
}

function recordingGatewayClient(onIterate: () => void): ProviderGatewayServiceClient {
  return {
    streamProviderRequest() {
      onIterate();
      return readableCall([]);
    },
  } as unknown as ProviderGatewayServiceClient;
}

function readableCall(events: Iterable<ProviderStreamEvent> | AsyncIterable<ProviderStreamEvent>): ReturnType<ProviderGatewayServiceClient["streamProviderRequest"]> {
  const readable = Readable.from(events, { objectMode: true }) as Readable & { cancel: () => void };
  readable.cancel = () => readable.destroy(Object.assign(new Error("cancelled"), { code: status.CANCELLED }));
  return readable as unknown as ReturnType<ProviderGatewayServiceClient["streamProviderRequest"]>;
}

function providerRequest(): ProviderRequest {
  return {
    requestId: "req_gateway_client",
    modelRequestId: "mreq_gateway_client",
    requestKind: ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
    workspaceId: "wksp_1",
    sessionId: "sesn_1",
    sessionThreadId: "thr_1",
    bindingId: "bind_1",
    bindingGeneration: 1,
    runtimeBindingToken: "binding-token",
    model: { providerId: "openai", modelId: "gpt-5.5", variant: "" },
    system: [{
      kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE,
      text: "System",
      cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_STABLE,
    }],
    context: [{
      role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
      content: [{ text: { text: "hello" } }],
    }],
    tools: [],
    attachments: [],
    limits: { maxOutputTokens: 128, timeoutMs: 30_000 },
  };
}
