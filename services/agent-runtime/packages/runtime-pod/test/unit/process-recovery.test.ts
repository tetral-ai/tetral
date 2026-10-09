import { expect, test } from "bun:test";
import { Metadata, Server, ServerCredentials, status } from "@grpc/grpc-js";
import {
  AgentRuntimeBridgeServiceService,
  RuntimeProcessPhase,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type { AgentRuntimeBridgeServiceServer } from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import { loadRuntimePodConfig } from "../../src/config.js";
import { RuntimePodLifecycle } from "../../src/lifecycle.js";
import { BridgeRuntimeProcess } from "../../src/runtime-process.js";
import { commandEnv } from "../fixtures/command-process.js";

test("actual current ACCEPTING ACK recovers expired freshness through a Bridge outage under all diagnostic sinks", async () => {
  for (const sink of ["normal", "silent", "throw"] as const) {
    await withLifecycle(sink, async (fixture) => {
      expect(fixture.lifecycle.ready()).toEqual({ ready: true });
      fixture.mode = "outage";
      await waitFor(() => !fixture.lifecycle.ready().ready);
      expect(fixture.lifecycle.metricsSnapshot().accepting).toBe(false);
      expect(() =>
        fixture.lifecycle.runCommand(async () => undefined),
      ).toThrow();
      fixture.mode = "healthy";
      await waitFor(() => fixture.lifecycle.ready().ready);
      expect(fixture.lifecycle.metricsSnapshot().accepting).toBe(true);
      expect(await fixture.lifecycle.runCommand(async () => "accepted")).toBe(
        "accepted",
      );
      if (sink === "normal") {
        expect(
          fixture.records.some(
            (record) => record.event === "runtime_process_freshness_expired",
          ),
        ).toBe(true);
        const failures = fixture.records.filter(
          (record) => record.event === "runtime_process_report_failed",
        );
        expect(failures).toHaveLength(1);
        expect(failures[0]).toMatchObject({
          operation: "runtime_process.report",
        });
        expect(failures[0]?.operation).not.toBe("shutdown");
        expect(failures[0]?.kind).not.toBe("shutdown_error");
        const recovered = fixture.records.filter(
          (record) => record.event === "runtime_process_report_recovered",
        );
        expect(recovered).toHaveLength(1);
        expect(Number(recovered[0]!["failed.count"])).toBeGreaterThanOrEqual(2);
      }
    });
  }
});

test("explicit stale rejection and a non-current ACK permanently fence the original boot", async () => {
  for (const mode of ["stale", "wrong-current", "wrong-process"] as const) {
    await withLifecycle("normal", async (fixture) => {
      fixture.mode = mode;
      await waitFor(() => !fixture.lifecycle.ready().ready);
      expect((await fixture.lifecycle.processFailure).code).toBe(
        status.FAILED_PRECONDITION,
      );
      fixture.mode = "healthy";
      const initial = fixture.acceptingAcks;
      await waitFor(() => fixture.acceptingAcks > initial + 1);
      await fixture.lifecycle.start();
      expect(fixture.lifecycle.ready()).toEqual({ ready: false });
      expect(fixture.lifecycle.metricsSnapshot().accepting).toBe(false);
      expect(() =>
        fixture.lifecycle.runCommand(async () => undefined),
      ).toThrow();
      expect(
        fixture.records.some(
          (record) => record.event === "runtime_process_report_recovered",
        ),
      ).toBe(false);
    });
  }
});

test("a previously dispatched ACCEPTING ACK cannot reopen shutdown admission", async () => {
  await withLifecycle("normal", async (fixture) => {
    const report = fixture.holdNextAccepting();
    await report.entered;
    const shutdown = fixture.lifecycle.shutdown();
    expect(fixture.lifecycle.ready()).toEqual({ ready: false });
    report.release();
    await shutdown;
    expect(fixture.lifecycle.ready()).toEqual({ ready: false });
    expect(fixture.lifecycle.metricsSnapshot().accepting).toBe(false);
    expect(() => fixture.lifecycle.runCommand(async () => undefined)).toThrow();
  });
});

async function withLifecycle(
  sink: "normal" | "silent" | "throw",
  operation: (fixture: {
    mode: "healthy" | "outage" | "stale" | "wrong-current" | "wrong-process";
    acceptingAcks: number;
    lifecycle: RuntimePodLifecycle;
    records: Array<Record<string, unknown>>;
    holdNextAccepting: () => { entered: Promise<void>; release: () => void };
  }) => Promise<void>,
) {
  const config = loadRuntimePodConfig({
    ...commandEnv(),
    TETRAL_RUNTIME_REPORT_TIMEOUT_MS: "30",
    TETRAL_RUNTIME_REPORT_INTERVAL_MS: "40",
    TETRAL_RUNTIME_PROCESS_FRESHNESS_MS: "120",
  });
  if (!config.ok) throw new Error("fixture config invalid");
  let mode: "healthy" | "outage" | "stale" | "wrong-current" | "wrong-process" =
    "healthy";
  let acceptingAcks = 0;
  let hold: { entered: () => void; release?: () => void } | undefined;
  const server = new Server();
  const implementation: Pick<
    AgentRuntimeBridgeServiceServer,
    "registerRuntimeProcess" | "reportRuntimeProcess"
  > = {
    registerRuntimeProcess(call, callback) {
      callback(null, {
        runtimeProcessId: call.request.runtimeProcessId,
        registrationOrder: 1,
        registrationReceipt: "fixture-receipt",
      });
    },
    reportRuntimeProcess(call, callback) {
      if (
        call.request.runtimeProcessId !== "fixture-boot" ||
        call.request.registrationReceipt !== "fixture-receipt"
      ) {
        callback({
          code: status.FAILED_PRECONDITION,
          message: "wrong process proof",
        });
        return;
      }
      if (
        call.request.phase ===
        RuntimeProcessPhase.RUNTIME_PROCESS_PHASE_ACCEPTING
      ) {
        if (mode === "outage") {
          callback({ code: status.UNAVAILABLE, message: "controlled outage" });
          return;
        }
        if (mode === "stale") {
          callback({
            code: status.FAILED_PRECONDITION,
            message: "superseded process",
          });
          return;
        }
        const acknowledge = () => {
          acceptingAcks++;
          callback(null, {
            runtimeProcessId:
              mode === "wrong-process"
                ? "another-boot"
                : call.request.runtimeProcessId,
            phase: call.request.phase,
            current: mode !== "wrong-current",
          });
        };
        if (hold !== undefined) {
          const waiting = hold;
          hold = undefined;
          waiting.release = acknowledge;
          waiting.entered();
          return;
        }
        acknowledge();
        return;
      }
      callback(null, {
        runtimeProcessId: call.request.runtimeProcessId,
        phase: call.request.phase,
        current: true,
      });
    },
  };
  server.addService(AgentRuntimeBridgeServiceService, implementation);
  const port = await new Promise<number>((resolve, reject) =>
    server.bindAsync(
      "127.0.0.1:0",
      ServerCredentials.createInsecure(),
      (error, port) => (error === null ? resolve(port) : reject(error)),
    ),
  );
  const records: Array<Record<string, unknown>> = [];
  const observe = (record: Record<string, unknown>) => {
    if (sink === "normal") records.push(record);
    if (sink === "throw") throw new Error("synthetic sink failure");
  };
  const lifecycle = new RuntimePodLifecycle({
    config,
    logger: { info: observe, error: observe },
    bootstrap: {
      runtime: async () => undefined,
      core: async () => undefined,
      grpc: async () => undefined,
      authClient: async () => undefined,
    },
    runtimeProcess: new BridgeRuntimeProcess("fixture-boot", {
      address: `127.0.0.1:${port}`,
      tokenPath: "/unused",
      policies: config.config.bridgeMethodPolicies,
      metadataFactory: async () => new Metadata(),
    }),
    shutdownHooks: { quiesce: async () => undefined },
  });
  const fixture = {
    lifecycle,
    records,
    get mode() {
      return mode;
    },
    set mode(value: typeof mode) {
      mode = value;
    },
    get acceptingAcks() {
      return acceptingAcks;
    },
    set acceptingAcks(value: number) {
      acceptingAcks = value;
    },
    holdNextAccepting: () => {
      let entered!: () => void;
      const pending = new Promise<void>((resolve) => {
        entered = resolve;
      });
      const waiting: { entered: () => void; release?: () => void } = {
        entered,
      };
      hold = waiting;
      return { entered: pending, release: () => waiting.release?.() };
    },
  };
  try {
    await lifecycle.start();
    await operation(fixture);
  } finally {
    await lifecycle.shutdown();
    await new Promise<void>((resolve) => server.tryShutdown(() => resolve()));
  }
}

async function waitFor(predicate: () => boolean) {
  const deadline = Date.now() + 1500;
  while (!predicate()) {
    if (Date.now() >= deadline)
      throw new Error("controlled lifecycle condition deadline exceeded");
    await Bun.sleep(5);
  }
}
