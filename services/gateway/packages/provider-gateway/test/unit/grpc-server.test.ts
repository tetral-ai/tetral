import { describe, expect, test } from "bun:test";
import { EventEmitter } from "node:events";
import { credentials, Metadata, status as grpcStatus } from "@grpc/grpc-js";
import {
  ProviderFinishReason,
  ProviderGatewayServiceClient,
  ProviderStreamEventType,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { createGatewayGrpcServer, writeProviderStreamEvents } from "../../src/grpc-server.js";
import { ProviderGatewayServiceShell } from "../../src/service.js";
import { validProviderRequest } from "./fixtures.js";
import type { ProviderStreamEvent } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { GatewayAuthenticator, ProviderRequestStreamer } from "../../src/service.js";
import type { StatusObject } from "@grpc/grpc-js";

describe("Gateway gRPC streaming transport", () => {
  test("streams many events through a real grpc-js client and ends cleanly", async () => {
    const request = validAnthropicProviderRequest();
    const service = createService({
      stream: async function* () {
        yield textEvent(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START, "");
        for (let index = 0; index < 32; index += 1) {
          yield textEvent(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_DELTA, `chunk-${index}`);
        }
        yield textEvent(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_END, "");
        yield {
          type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
          finish: {
            reason: ProviderFinishReason.PROVIDER_FINISH_REASON_STOP,
            usage: {
              inputTotalTokens: 1,
              inputUncachedTokens: 1,
              outputTotalTokens: 2,
              totalTokens: 3,
              providerUsageJson: "{}",
            },
            metadataJson: "{}",
          },
        };
      },
    });
    const server = createGatewayGrpcServer(service);
    const port = await server.bind("127.0.0.1:0");
    const client = new ProviderGatewayServiceClient(`127.0.0.1:${port}`, credentials.createInsecure());
    try {
      const call = client.streamProviderRequest(request, metadata());
      const finalStatusPromise = onceStatus(call);
      const events = await readStream(call);
      const finalStatus = await finalStatusPromise;

      expect(events).toHaveLength(35);
      expect(events[0]?.type).toBe(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START);
      expect(events[34]?.type).toBe(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH);
      expect(finalStatus.code).toBe(grpcStatus.OK);
      expect(finalStatus.metadata).toBeInstanceOf(Metadata);
      expect(finalStatus.metadata.getMap()).toEqual({});
    } finally {
      client.close();
      await server.shutdown();
    }
  });

  test("delivers pre-event INVALID_ARGUMENT as terminal grpc status", async () => {
    const service = createService({ stream: async function* () {} });
    const server = createGatewayGrpcServer(service);
    const port = await server.bind("127.0.0.1:0");
    const client = new ProviderGatewayServiceClient(`127.0.0.1:${port}`, credentials.createInsecure());
    try {
      const request = { ...validAnthropicProviderRequest(), requestId: "" };
      const error = await Promise.race([
        readStream(client.streamProviderRequest(request, metadata())).then(
          () => undefined,
          (failure: unknown) => failure,
        ),
        new Promise<never>((_resolve, reject) =>
          setTimeout(() => reject(new Error("timed out waiting for terminal grpc status")), 1_000)),
      ]);
      expect(error).toMatchObject({ code: grpcStatus.INVALID_ARGUMENT });
    } finally {
      client.close();
      await server.shutdown();
    }
  });

  test("carries the largest advertised tool-input shapes across the real stream", async () => {
    const cases = [
      {
        name: "memory-create",
        inputJson: JSON.stringify({ path: "notes.txt", content: "\u0001".repeat(102_400) }),
      },
      {
        name: "memory-replace",
        inputJson: JSON.stringify({
          path: "notes.txt",
          old_text: "\u0001".repeat(102_400),
          new_text: "\u0001".repeat(102_400),
        }),
      },
      {
        name: "web-eight-operations",
        inputJson: JSON.stringify({
          search_query: Array.from({ length: 8 }, () => ({ q: "\u0001".repeat(64 * 1024), domains: ["example.test"] })),
        }),
      },
    ];
    for (const scenario of cases) {
      const request = validAnthropicProviderRequest();
      const service = createService({
        stream: async function* () {
          yield {
            type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_CALL,
            toolCall: { id: `call_${scenario.name}`, name: "bounded_tool", inputJson: scenario.inputJson, metadataJson: "{}" },
          };
        },
      });
      const server = createGatewayGrpcServer(service);
      const port = await server.bind("127.0.0.1:0");
      const client = new ProviderGatewayServiceClient(`127.0.0.1:${port}`, credentials.createInsecure(), {
        "grpc.max_receive_message_length": 8 * 1024 * 1024,
      });
      try {
        const events = await readStream(client.streamProviderRequest(request, metadata()));
        expect(events, scenario.name).toHaveLength(1);
        expect(events[0]?.toolCall?.inputJson, scenario.name).toBe(scenario.inputJson);
      } finally {
        await server.shutdown();
        client.close();
      }
    }
  }, 15_000);

  test("honors writable backpressure before consuming the next provider event", async () => {
    const first = textEvent(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_DELTA, "one");
    const second = textEvent(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_DELTA, "two");
    const call = new BackpressureCall();
    const writeTask = writeProviderStreamEvents(call, asyncEvents([first, second]));

    await call.waitForWrites(1);
    expect(call.writes).toHaveLength(1);
    call.emit("drain");
    await writeTask;

    expect(call.writes).toEqual([first, second]);
    expect(call.listenerCount("drain")).toBe(0);
    expect(call.listenerCount("cancelled")).toBe(0);
  });

  test("stops waiting for drain when the client cancels during backpressure", async () => {
    const first = textEvent(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_DELTA, "one");
    const second = textEvent(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_DELTA, "two");
    const call = new BackpressureCall();
    const writeTask = writeProviderStreamEvents(call, asyncEvents([first, second]));

    await call.waitForWrites(1);
    call.cancelled = true;
    call.emit("cancelled");
    await writeTask;

    expect(call.writes).toEqual([first]);
    expect(call.listenerCount("drain")).toBe(0);
    expect(call.listenerCount("cancelled")).toBe(0);
  });

  test("client cancellation aborts the upstream provider stream", async () => {
    let aborted = false;
    let release: (() => void) | undefined;
    const statuses: StatusObject[] = [];
    const request = validAnthropicProviderRequest();
    const service = createService({
      stream: async function* (input) {
        input.abortSignal?.addEventListener("abort", () => {
          aborted = true;
          release?.();
        }, { once: true });
        yield textEvent(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START, "");
        await new Promise<void>((resolve) => {
          release = resolve;
        });
      },
    });
    const server = createGatewayGrpcServer(service);
    const port = await server.bind("127.0.0.1:0");
    const client = new ProviderGatewayServiceClient(`127.0.0.1:${port}`, credentials.createInsecure());
    try {
      const call = client.streamProviderRequest(request, metadata());
      call.on("error", () => undefined);
      call.on("status", (value) => statuses.push(value));
      await onceData(call);
      call.cancel();
      await waitUntil(() => aborted && statuses.length > 0);
      expect(aborted).toBe(true);
      expect(statuses).toHaveLength(1);
      expect(statuses[0]?.code).toBe(grpcStatus.CANCELLED);
    } finally {
      await server.shutdown();
      client.close();
    }
  });
});

function createService(providerStreamer: ProviderRequestStreamer): ProviderGatewayServiceShell {
  return new ProviderGatewayServiceShell({
    authenticator: new AllowingAuthenticator(),
    runtimeBindingTokenVerifier: { verify: () => true },
    ready: () => true,
    logger: { info: () => undefined, error: () => undefined },
    providerStreamer,
  });
}

class AllowingAuthenticator implements GatewayAuthenticator {
  async authenticate() {
    return {
      ok: true as const,
      serviceAccount: { namespace: "tetral-agent-runtime", name: "agent-runtime", podUid: "pod_uid_gateway" },
    };
  }
}

function metadata(): Metadata {
  const value = new Metadata();
  value.set("authorization", "bearer request-token");
  return value;
}

function validAnthropicProviderRequest() {
  return validProviderRequest({
    model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "" },
  });
}

function textEvent(
  type: ProviderStreamEventType,
  text: string,
): ProviderStreamEvent {
  return {
    type,
    text: {
      id: "text_1",
      text,
      metadataJson: "{}",
    },
  };
}

async function readStream(events: AsyncIterable<ProviderStreamEvent>): Promise<readonly ProviderStreamEvent[]> {
  const out: ProviderStreamEvent[] = [];
  for await (const event of events) {
    out.push(event);
  }
  return out;
}

async function* asyncEvents(events: readonly ProviderStreamEvent[]): AsyncGenerator<ProviderStreamEvent> {
  for (const event of events) {
    yield event;
  }
}

class BackpressureCall extends EventEmitter {
  cancelled = false;
  readonly writes: ProviderStreamEvent[] = [];
  private waiters: Array<() => void> = [];

  write(event: ProviderStreamEvent): boolean {
    this.writes.push(event);
    for (const waiter of this.waiters.splice(0)) {
      waiter();
    }
    return this.writes.length !== 1;
  }

  async waitForWrites(count: number): Promise<void> {
    while (this.writes.length < count) {
      await new Promise<void>((resolve) => this.waiters.push(resolve));
    }
  }
}

async function onceData(call: EventEmitter): Promise<void> {
  await new Promise<void>((resolve) => call.once("data", () => resolve()));
}

function onceStatus(call: { once: (event: "status", listener: (value: StatusObject) => void) => unknown }): Promise<StatusObject> {
  return new Promise<StatusObject>((resolve) => call.once("status", resolve));
}

async function waitUntil(predicate: () => boolean): Promise<void> {
  const started = Date.now();
  while (!predicate()) {
    if (Date.now() - started > 1_000) {
      throw new Error("condition did not become true");
    }
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}
