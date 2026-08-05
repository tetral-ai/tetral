import { describe, expect, test } from "bun:test";
import { Metadata, status } from "@grpc/grpc-js";
import {
  ProviderRequestKind,
  RuntimeMessageRole,
  SystemCacheHint,
  SystemSegmentKind,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type {
  ProviderGatewayServiceClient,
  ProviderRequest,
  ProviderStreamEvent,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { Effect, Stream } from "effect";
import { MaxGatewayRequestGrpcMessageBytes, MaxGatewayStreamEventGrpcMessageBytes } from "../../src/bounds.js";
import { RuntimePodGatewayClient } from "../../src/gateway-client.js";

describe("Runtime Pod Gateway client", () => {
  test("logs one bounded identity record for a failure before the first event", async () => {
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
      event: "provider_stream_failed_before_first_event",
      "workspace.id": request.workspaceId,
      "session.id": request.sessionId,
      "thread.id": request.sessionThreadId,
      "request.id": request.requestId,
      "provider.request.id": request.modelRequestId,
      "error.code": "gateway_protocol_error",
    })]);
    expect(JSON.stringify(records)).not.toContain("secret raw rejection");
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

    const error = await Effect.runPromise(
      Stream.runCollect(client.streamProviderRequest(providerRequest(), {
        abortSignal: abortController.signal,
      })).pipe(Effect.flip),
    );

    expect(error).toMatchObject({
      type: "gateway-client",
      code: "gateway_cancelled",
      retryable: false,
      fatal: false,
    });
    expect(records).toEqual([]);
  });

  test("rejects an oversized ProviderRequest before metadata or transport work", async () => {
    let metadataCalls = 0;
    let transportCalls = 0;
    const records: unknown[] = [];
    const request = providerRequest();
    request.messages[0]!.parts = [{
      id: "part_oversized",
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
      event: "provider_stream_failed_before_first_event",
      "request.id": request.requestId,
      "provider.request.id": request.modelRequestId,
      "error.code": "gateway_protocol_error",
    })]);
  });

  test("keeps peer receive-limit details in the remote retryable arm", async () => {
    for (const details of [
      "Received message larger than max (33554433 vs 33554432)",
      "Received message that decompresses to a size larger than 33554432",
    ]) {
      const error = await collectGatewayError(new RuntimePodGatewayClient({
        address: "gateway.test:9090",
        tokenPath: "/var/run/token",
        client: failingGatewayClient(status.RESOURCE_EXHAUSTED, details),
        metadataFactory: async () => new Metadata(),
      }), providerRequest());

      expect(error).toMatchObject({
        code: "gateway_unavailable",
        retryable: true,
        fatal: false,
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
});

async function collectGatewayError(client: RuntimePodGatewayClient, request: ProviderRequest) {
  return await Effect.runPromise(
    Stream.runCollect(client.streamProviderRequest(request)).pipe(Effect.flip),
  );
}

function failingGatewayClient(code: number, details = "gateway request failed"): ProviderGatewayServiceClient {
  return recordingGatewayClient(() => {
    throw Object.assign(new Error(details), { code, details });
  });
}

function recordingGatewayClient(onIterate: () => void): ProviderGatewayServiceClient {
  return {
    streamProviderRequest() {
      return {
        cancel() {},
        async *[Symbol.asyncIterator](): AsyncIterator<ProviderStreamEvent> {
          onIterate();
        },
      };
    },
  } as unknown as ProviderGatewayServiceClient;
}

function providerRequest(): ProviderRequest {
  return {
    requestId: "req_gateway_client",
    modelRequestId: "mreq_gateway_client",
    requestKind: ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
    workspaceId: "wksp_1",
    sessionId: "sesn_1",
    sessionThreadId: "thr_1",
    parentThreadId: undefined,
    bindingId: "bind_1",
    bindingGeneration: 1,
    runtimeBindingToken: "binding-token",
    model: { providerId: "openai", modelId: "gpt-5.5", variant: "" },
    system: [{
      kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE,
      text: "System",
      cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_STABLE,
    }],
    messages: [{
      id: "msg_1",
      role: RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_USER,
      status: "completed",
      origin: "user",
      parts: [{ id: "part_1", text: { text: "hello" } }],
    }],
    tools: [],
    attachments: [],
    limits: { maxOutputTokens: 128, timeoutMs: 30_000 },
  };
}
