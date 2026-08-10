import { describe, expect, test } from "bun:test";
import { Metadata, Server, ServerCredentials } from "@grpc/grpc-js";
import type { ServerWritableStream } from "@grpc/grpc-js";
import {
  ProviderGatewayServiceService,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type {
  ProviderGatewayServiceServer,
  ProviderRequest,
  ProviderStreamEvent,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { Effect, Fiber } from "effect";
import type {
  RuntimeDependencies,
  SessionEventWriter,
  SessionEventWriterRequestEndEnvelope,
} from "../../../core/src/contracts/runtime.js";
import { createLLMService } from "../../../core/src/llm/llm-service.js";
import { DefaultProviderCallRuntimeConfig } from "../../../core/src/thread-loop/provider-request.js";
import { ThreadRuntime } from "../../../core/src/thread-loop/thread-runtime.js";
import * as ThreadLoop from "../../../core/src/thread-loop/thread-loop.js";
import { buildThreadLoopUserMessage as userMessage } from "../../../core/test/unit/runtime-message-builders.js";
import {
  RecordingContextLoader,
  beginTestUserInterrupt,
  createdAt,
  deferred,
  runtimeThreadLoopLayer,
  testRunCustody,
  threadLoopRuntime,
  waitForCondition,
  writerFrom,
} from "../../../core/test/unit/thread-loop/thread-loop-test-support.js";
import {
  RuntimeGatewayTransportCompletionAllowanceMs,
  RuntimePodGatewayClient,
  runtimeProviderStreamObserver,
} from "../../src/gateway-client.js";
import type { RuntimePodLogRecord, RuntimePodLogger } from "../../src/logger.js";

interface SilentGateway {
  readonly address: string;
  readonly cancelledCalls: () => number;
  readonly callOpened: Promise<void>;
  readonly close: () => Promise<void>;
}

interface RaceFixture {
  readonly deadline: () => void;
  readonly deadlineDelayMs: number;
  readonly loggerRecords: readonly RuntimePodLogRecord[];
  readonly requestEnds: readonly SessionEventWriterRequestEndEnvelope[];
  readonly reschedules: readonly ThreadLoop.RuntimeProviderRescheduleObservation[];
  readonly interruptRun: () => Promise<void>;
  readonly session: ThreadRuntime;
  readonly gateway: SilentGateway;
  readonly releaseReschedule: () => void;
}

describe("Gateway completion and durable interrupt linearization", () => {
  test("durable interrupt wins before the completion deadline", async () => {
    const fixture = await startRaceFixture("interrupt_first");
    try {
      await fixture.gateway.callOpened;
      beginTestUserInterrupt(fixture.session, "rin_interrupt_before_gateway_deadline");
      const interrupted = fixture.interruptRun();
      await waitForCondition(() => fixture.requestEnds.length === 1, "interrupted Request End");
      fixture.deadline();
      fixture.releaseReschedule();

      await interrupted;
      expect(fixture.requestEnds).toHaveLength(1);
      expect(fixture.requestEnds[0]).toMatchObject({
        isError: true,
        errorKind: "runtime_interrupted",
        finishReason: "cancelled",
      });
      expect(fixture.reschedules).toEqual([]);
      await waitForCondition(() => fixture.gateway.cancelledCalls() === 1, "single cancelled gRPC call");
      expect(closeOutcomes(fixture.loggerRecords)).toEqual(["cancelled"]);
      expect(closeRecords(fixture.loggerRecords)[0]).toMatchObject({ "cancel.kind": "caller" });
      expect(streamRecordKinds(fixture.loggerRecords)).toEqual([
        "runtime_provider_stream_opened",
        "runtime_provider_stream_closed",
      ]);
      expect(JSON.stringify(fixture.loggerRecords)).not.toContain("raw provider payload marker");
    } finally {
      await fixture.gateway.close();
    }
  });

  test("completion deadline wins before durable interrupt admission", async () => {
    const fixture = await startRaceFixture("deadline_first");
    try {
      await fixture.gateway.callOpened;
      fixture.deadline();
      await waitForCondition(() => fixture.requestEnds.length === 1, "deadline Request End");
      beginTestUserInterrupt(fixture.session, "rin_interrupt_after_gateway_deadline");
      const interrupted = fixture.interruptRun();
      fixture.releaseReschedule();

      await interrupted;
      expect(fixture.requestEnds).toHaveLength(1);
      expect(fixture.requestEnds[0]).toMatchObject({
        isError: true,
        errorKind: "gateway_stream_error",
        reschedule: { attempt: 1, backoffMs: 1_000 },
      });
      expect(fixture.reschedules).toEqual([
        expect.objectContaining({
          attempt: 1,
          failureCode: "gateway_stream_error",
        }),
      ]);
      await waitForCondition(() => fixture.gateway.cancelledCalls() === 1, "single cancelled gRPC call");
      expect(closeOutcomes(fixture.loggerRecords)).toEqual(["completion_deadline"]);
      expect(streamRecordKinds(fixture.loggerRecords)).toEqual([
        "runtime_provider_stream_opened",
        "runtime_provider_stream_closed",
      ]);
      expect(JSON.stringify(fixture.loggerRecords)).not.toContain("raw provider payload marker");
    } finally {
      await fixture.gateway.close();
    }
  });
});

async function startRaceFixture(name: string): Promise<RaceFixture> {
  const gateway = await startSilentGateway();
  const callbacks: Array<() => void> = [];
  const scheduledDurations: number[] = [];
  const callClock = Date.now();
  const loggerRecords: RuntimePodLogRecord[] = [];
  const logger: RuntimePodLogger = {
    info(record) {
      loggerRecords.push(record);
      throw new Error("throwing test logger");
    },
    error() {
      throw new Error("throwing test logger");
    },
  };
  const client = new RuntimePodGatewayClient({
    address: gateway.address,
    tokenPath: "/unused",
    metadataFactory: async () => new Metadata(),
    logger,
    nowEpochMs: () => callClock,
    scheduleTimeout(callback, durationMs) {
      callbacks.push(callback);
      scheduledDurations.push(durationMs);
      return setTimeout(() => undefined, 60_000);
    },
    cancelTimeout: clearTimeout,
  });
  const session = new ThreadRuntime(`sesn_gateway_race_${name}`);
  const loader = new RecordingContextLoader([], {
    type: "messages",
    messages: [userMessage(`user_gateway_race_${name}`, 0, "raw provider payload marker")],
  });
  const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
  const reschedules: ThreadLoop.RuntimeProviderRescheduleObservation[] = [];
  const rescheduleWait = deferred<void>();
  const baseWriter = writerFrom((envelope) => ({
    ok: true,
    writeId: envelope.writeId,
    eventId: `bridge-${envelope.writeId}`,
    processedAt: createdAt,
  }));
  const writer: SessionEventWriter = {
    ...baseWriter,
    writeRequestEnd: async (envelope) => {
      requestEnds.push(envelope);
      return await baseWriter.writeRequestEnd(envelope);
    },
  };
  const runtime: RuntimeDependencies = {
    ...threadLoopRuntime(),
    sleep: async (_durationMs, signal) => await Promise.race([
      rescheduleWait.promise.then(() => true),
      new Promise<false>((resolve) => {
        if (signal.aborted) {
          resolve(false);
          return;
        }
        signal.addEventListener("abort", () => resolve(false), { once: true });
      }),
    ]),
  };
  const llmService = createLLMService(client, runtimeProviderStreamObserver(logger, () => callClock));
  const runFiber = Effect.runFork(Effect.gen(function* () {
    return yield* (yield* ThreadLoop.Service).run(session, testRunCustody());
  }).pipe(Effect.provide(runtimeThreadLoopLayer(loader, {
    writer,
    llmService,
    runtime,
    providerCallRuntime: { ...DefaultProviderCallRuntimeConfig, timeoutMs: 25 },
    runtimePolicy: () => ({ providerRescheduleBudget: 3, compactionRescheduleBudget: 2 }),
    recordProviderReschedule: (event) => reschedules.push(event),
  }))));
  await gateway.callOpened;
  await waitForCondition(() => callbacks.length === 1, "Runtime completion timer");
  expect(scheduledDurations).toEqual([25 + RuntimeGatewayTransportCompletionAllowanceMs]);
  return {
    deadline: callbacks[0]!,
    deadlineDelayMs: scheduledDurations[0]!,
    loggerRecords,
    requestEnds,
    reschedules,
    interruptRun: async () => {
      await Effect.runPromise(Fiber.interrupt(runFiber));
    },
    session,
    gateway,
    releaseReschedule: () => rescheduleWait.resolve(),
  };
}

async function startSilentGateway(): Promise<SilentGateway> {
  const opened = deferred<void>();
  let cancelledCalls = 0;
  const implementation: ProviderGatewayServiceServer = {
    streamProviderRequest(call: ServerWritableStream<ProviderRequest, ProviderStreamEvent>) {
      call.on("cancelled", () => {
        cancelledCalls += 1;
      });
      opened.resolve();
    },
    runWeb() {
      throw new Error("not implemented");
    },
  };
  const server = new Server();
  server.addService(ProviderGatewayServiceService, implementation);
  const port = await new Promise<number>((resolve, reject) => {
    server.bindAsync("127.0.0.1:0", ServerCredentials.createInsecure(), (error, boundPort) => {
      if (error !== null) {
        reject(error);
        return;
      }
      resolve(boundPort);
    });
  });
  return {
    address: `127.0.0.1:${port}`,
    cancelledCalls: () => cancelledCalls,
    callOpened: opened.promise,
    close: async () => {
      server.forceShutdown();
    },
  };
}

function closeOutcomes(records: readonly RuntimePodLogRecord[]): readonly unknown[] {
  return closeRecords(records)
    .map((record) => record["transport.outcome"]);
}

function closeRecords(records: readonly RuntimePodLogRecord[]): readonly RuntimePodLogRecord[] {
  return records.filter((record) => record.event === "runtime_provider_stream_closed");
}

function streamRecordKinds(records: readonly RuntimePodLogRecord[]): readonly string[] {
  return records
    .filter((record) => record.event.startsWith("runtime_provider_"))
    .map((record) => record.event);
}
