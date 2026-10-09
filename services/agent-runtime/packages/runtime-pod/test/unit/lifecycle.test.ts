import { describe, expect, test } from "bun:test";
import { status } from "@grpc/grpc-js";
import { RuntimeHandoffDisposition } from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type { RuntimeQuiesceOptions } from "@tetral/agent-runtime-core/src/session/session-manager.js";
import { loadRuntimePodConfig } from "../../src/config.js";
import { createJsonLogger } from "../../src/logger.js";
import { GrpcStatusError, RuntimePodLifecycle } from "../../src/lifecycle.js";
import type { RuntimeProcessPort } from "../../src/runtime-process.js";

describe("Runtime Pod lifecycle", () => {
  test("registration and accepting ACK gate readiness; freshness expires between reports", async () => {
    const config = loadRuntimePodConfig({
      ...validEnv(),
      TETRAL_RUNTIME_REPORT_TIMEOUT_MS: "10",
      TETRAL_RUNTIME_REPORT_INTERVAL_MS: "20",
      TETRAL_RUNTIME_PROCESS_FRESHNESS_MS: "60",
    });
    expect(config.ok).toBe(true);
    let acceptingAck!: () => void;
    const accepting = new Promise<void>((resolve) => {
      acceptingAck = resolve;
    });
    let reports = 0;
    const phases: string[] = [];
    const lifecycle = new RuntimePodLifecycle({
      config,
      logger: { info: () => undefined, error: () => undefined },
      bootstrap: successfulBootstrap(),
      runtimeProcess: {
        runtimeProcessId: "boot-freshness",
        register: async () => {
          phases.push("registered");
        },
        report: async (phase) => {
          phases.push(phase);
          if (phase === "accepting" && reports++ === 0) await accepting;
          else if (phase === "accepting") throw new Error("report unavailable");
        },
        release: async () => {
          throw new Error("unexpected release");
        },
        close: async () => undefined,
      },
      shutdownHooks: { quiesce: async () => undefined },
    });
    const startup = lifecycle.start();
    await waitFor(() => phases.length === 2, "registration and accepting report");
    expect(phases).toEqual(["registered", "accepting"]);
    expect(lifecycle.ready()).toEqual({ ready: false });
    acceptingAck();
    await startup;
    expect(lifecycle.ready()).toEqual({ ready: true });
    await new Promise((resolve) => setTimeout(resolve, 85));
    expect(lifecycle.metricsSnapshot().accepting).toBe(false);
    expect(lifecycle.ready()).toEqual({ ready: false });
    expect(() => lifecycle.runCommand(async () => undefined)).toThrow();
    await lifecycle.shutdown();
    expect(phases).toContain("draining");
  });

  test("expired cancellation joins producer and Core before process client close", async () => {
    let releaseProducer!: () => void, releaseCore!: () => void;
    const producer = new Promise<void>((resolve) => {
      releaseProducer = resolve;
    });
    const core = new Promise<void>((resolve) => {
      releaseCore = resolve;
    });
    const producerSettled = deferred<void>("producer settlement");
    let closed = false,
      joined = false;
    const lifecycle = new RuntimePodLifecycle({
      config: shortShutdownConfig(),
      logger: { info: () => undefined, error: () => undefined },
      bootstrap: successfulBootstrap(),
      runtimeProcess: {
        runtimeProcessId: "boot-joined",
        register: async () => undefined,
        report: async () => undefined,
        release: async () => {
          throw new Error("unexpected release");
        },
        close: async () => {
          closed = true;
        },
      },
      shutdownHooks: {
        quiesce: async () => {
          await core;
        },
      },
    });
    await lifecycle.start();
    const result = lifecycle
      .runCommand(async () => {
        try {
          await producer;
        } finally {
          producerSettled.resolve(undefined);
        }
      })
      .catch((error) => error);
    const shutdown = lifecycle.shutdown().then(() => {
      joined = true;
    });
    // The public command promise rejects when the settlement window expires.
    expect((await result).code).toBe(status.FAILED_PRECONDITION);
    expect(closed).toBe(false);
    expect(joined).toBe(false);
    releaseProducer();
    await producerSettled.promise;
    // Runtime Core is still held, so the process client must remain open.
    expect(closed).toBe(false);
    releaseCore();
    await shutdown;
    expect(closed).toBe(true);
    expect(joined).toBe(true);
  });

  test("heartbeat first failure, count, and recovery survive silent and throwing sinks", async () => {
    for (const sink of ["normal", "silent", "throw"] as const) {
      const config = loadRuntimePodConfig({
        ...validEnv(),
        TETRAL_RUNTIME_REPORT_TIMEOUT_MS: "2",
        TETRAL_RUNTIME_REPORT_INTERVAL_MS: "5",
        TETRAL_RUNTIME_PROCESS_FRESHNESS_MS: "100",
      });
      const records: Array<Record<string, unknown>> = [];
      const observe = (record: Record<string, unknown>) => {
        if (sink === "normal") records.push(record);
        if (sink === "throw") throw new Error("sink unavailable");
      };
      let reports = 0;
      const lifecycle = new RuntimePodLifecycle({
        config,
        logger: { info: observe, error: observe },
        bootstrap: successfulBootstrap(),
        runtimeProcess: {
          runtimeProcessId: "boot-reports",
          register: async () => undefined,
          report: async (phase) => {
            if (phase === "accepting" && ++reports > 1 && reports < 5)
              throw new Error("report unavailable");
          },
          release: async () => {
            throw new Error("unexpected release");
          },
          close: async () => undefined,
        },
        shutdownHooks: { quiesce: async () => undefined },
      });
      await lifecycle.start();
      await waitFor(() => reports >= 5, "five accepting reports");
      expect(reports).toBeGreaterThanOrEqual(5);
      expect(lifecycle.ready()).toEqual({ ready: true });
      await lifecycle.shutdown();
      if (sink === "normal") {
        const failures = records.filter(
            (record) => record.event === "runtime_process_report_failed",
          ),
          recovered = records.filter(
            (record) => record.event === "runtime_process_report_recovered",
          );
        expect(failures).toHaveLength(1);
        expect(failures[0]).toMatchObject({
          "failed.count": 1,
          operation: "runtime_process.report",
          "error.code": "runtime_process_report_failed",
        });
        expect(failures[0]?.operation).not.toBe("shutdown");
        expect(failures[0]?.kind).not.toBe("shutdown_error");
        expect(recovered).toHaveLength(1);
        expect(recovered[0]?.["failed.count"]).toBe(3);
      }
    }
  });

  test("health is OK and readiness flips true only after all bootstrap gates succeed", async () => {
    const lifecycle = new RuntimePodLifecycle({
      config: validConfig(),
      logger: createJsonLogger({ write: () => undefined }),
      bootstrap: {
        runtime: async () => undefined,
        core: async () => undefined,
        grpc: async () => undefined,
        authClient: async () => undefined,
      },
      runtimeProcess: fakeRuntimeProcess(),
      shutdownHooks: idleCore(),
    });

    expect(lifecycle.health()).toEqual({ ok: true });
    expect(lifecycle.ready()).toEqual({ ready: false });

    await lifecycle.start();

    expect(lifecycle.ready()).toEqual({ ready: true });
    await lifecycle.shutdown();
  });

  test("a throwing diagnostic callback cannot retain ready admission after failed bootstrap", async () => {
    let failCore = false;
    const lifecycle = new RuntimePodLifecycle({
      config: validConfig(),
      logger: {
        info: () => undefined,
        error: () => {
          throw new Error("PRIVATE_LIFECYCLE_LOG_SENTINEL");
        },
      },
      bootstrap: {
        ...successfulBootstrap(),
        core: async () => {
          if (failCore) throw new Error("PRIVATE_BOOTSTRAP_SENTINEL");
        },
      },
      runtimeProcess: fakeRuntimeProcess(),
      shutdownHooks: idleCore(),
    });
    await lifecycle.start();
    expect(lifecycle.ready()).toEqual({ ready: true });
    failCore = true;
    await lifecycle.start();
    expect(lifecycle.ready()).toEqual({ ready: false });
    expect(lifecycle.metricsSnapshot()).toEqual({
      ready: false,
      accepting: false,
      inFlightCommands: 0,
    });
    expect(() => lifecycle.runCommand(async () => "must not run")).toThrow(
      "runtime pod shutting down",
    );
    await lifecycle.shutdown();
  });

  test("config/env failure is classified as config_error and readiness remains false", async () => {
    const sink: string[] = [];
    const parsed = loadRuntimePodConfig({
      ...validEnv(),
      TETRAL_RUNTIME_POD_IP: "runtime.service.local",
    });

    expect(parsed.ok).toBe(false);
    if (!parsed.ok) {
      expect(parsed.error.kind).toBe("config_error");
      expect(parsed.error.message).toBe("invalid runtime pod identity");
      expect(JSON.stringify(parsed.error)).not.toContain("runtime.service.local");
    }

    const lifecycle = new RuntimePodLifecycle({
      config: parsed,
      logger: createJsonLogger({ write: (line) => sink.push(line) }),
      bootstrap: successfulBootstrap(),
      runtimeProcess: fakeRuntimeProcess(),
      shutdownHooks: idleCore(),
    });
    await lifecycle.start();

    expect(lifecycle.ready()).toEqual({ ready: false });
    expect(sink.join("\n")).toContain("config_error");
    expect(sink.join("\n")).not.toContain("runtime.service.local");
  });

  test("runtime config requires the platform approval reviewer model and skill budget", async () => {
    const parsed = loadRuntimePodConfig(validEnv());

    expect(parsed.ok).toBe(true);
    if (parsed.ok) {
      expect(parsed.config.platformModels).toEqual({
        approvalReviewer: { providerId: "anthropic", modelId: "claude-opus-4-8" },
      });
      expect(parsed.config.skillGuidance.descriptionBudgetBytes).toBe(32_768);
      expect(parsed.config.providerStreamTimeoutMs).toBe(1_800_000);
    }

    expect(
      loadRuntimePodConfig({ ...validEnv(), TETRAL_RUNTIME_APPROVAL_REVIEWER_MODEL: "anthropic/" })
        .ok,
    ).toBe(false);
    expect(
      loadRuntimePodConfig({
        ...validEnv(),
        TETRAL_RUNTIME_SKILL_GUIDANCE_DESCRIPTION_BUDGET_BYTES: "65536",
      }).ok,
    ).toBe(false);
    expect(
      loadRuntimePodConfig({ ...validEnv(), TETRAL_RUNTIME_PROVIDER_STREAM_TIMEOUT_MS: "0" }).ok,
    ).toBe(false);
    expect(
      loadRuntimePodConfig({ ...validEnv(), TETRAL_RUNTIME_PROVIDER_STREAM_TIMEOUT_MS: "1.5" }).ok,
    ).toBe(false);
    expect(
      loadRuntimePodConfig({
        ...validEnv(),
        TETRAL_RUNTIME_PROVIDER_STREAM_TIMEOUT_MS: "2147483647",
      }).ok,
    ).toBe(true);
    expect(
      loadRuntimePodConfig({
        ...validEnv(),
        TETRAL_RUNTIME_PROVIDER_STREAM_TIMEOUT_MS: "2147483648",
      }).ok,
    ).toBe(false);
  });

  test("cross-field configuration failures name only the keys involved", () => {
    for (const scenario of [
      {
        env: { TETRAL_RUNTIME_POD_GRPC_PORT: "19091" },
        message: "TETRAL_RUNTIME_POD_GRPC_PORT must equal the TETRAL_TRANSPORT_PROFILE port",
        value: "19091",
      },
      {
        env: { TETRAL_TRANSPORT_PROFILE: "hardened", TETRAL_RUNTIME_POD_GRPC_PORT: "19090" },
        message: "TETRAL_RUNTIME_POD_GRPC_PORT must equal the TETRAL_TRANSPORT_PROFILE port",
        value: "19090",
      },
      {
        env: { TETRAL_RUNTIME_REPORT_TIMEOUT_MS: "2500" },
        message:
          "TETRAL_RUNTIME_REPORT_TIMEOUT_MS must be shorter than TETRAL_RUNTIME_REPORT_INTERVAL_MS",
        value: "2500",
      },
      {
        env: { TETRAL_RUNTIME_REPORT_INTERVAL_MS: "12500" },
        message:
          "TETRAL_RUNTIME_REPORT_INTERVAL_MS must be shorter than TETRAL_RUNTIME_PROCESS_FRESHNESS_MS",
        value: "12500",
      },
    ]) {
      const parsed = loadRuntimePodConfig({ ...validEnv(), ...scenario.env });
      expect(parsed.ok).toBe(false);
      if (!parsed.ok) {
        expect(parsed.error).toEqual({ kind: "config_error", message: scenario.message });
        expect(JSON.stringify(parsed.error)).not.toContain(scenario.value);
      }
    }
  });

  test("dependency, listener, and auth-client failures are startup_error without raw details", async () => {
    for (const scenario of [
      {
        name: "dependency",
        bootstrap: {
          ...successfulBootstrap(),
          core: async () => {
            throw new Error("postgres://secret@host/db raw provider payload sk-provider-key");
          },
        },
      },
      {
        name: "listener",
        bootstrap: {
          ...successfulBootstrap(),
          grpc: async () => {
            throw new Error("127.0.0.1:19090 bind failed raw request body");
          },
        },
      },
      {
        name: "auth",
        bootstrap: {
          ...successfulBootstrap(),
          authClient: async () => {
            throw new Error(
              `bearer secret-token https://kubernetes.default.svc {"kind":"TokenReview","status":{"error":"kube object dump"}}`,
            );
          },
        },
      },
    ]) {
      const sink: string[] = [];
      const lifecycle = new RuntimePodLifecycle({
        config: validConfig(),
        logger: createJsonLogger({ write: (line) => sink.push(line) }),
        bootstrap: scenario.bootstrap,
        runtimeProcess: fakeRuntimeProcess(),
        shutdownHooks: idleCore(),
      });

      await lifecycle.start();

      const output = sink.join("\n");
      expect(lifecycle.ready(), scenario.name).toEqual({ ready: false });
      expect(output).toContain("startup_error");
      for (const forbidden of [
        "postgres://",
        "127.0.0.1",
        "secret-token",
        "raw provider payload",
        "sk-provider-key",
        "raw request body",
        "kubernetes.default.svc",
        "TokenReview",
        "kube object dump",
      ]) {
        expect(output).not.toContain(forbidden);
      }
    }
  });

  test("shutdown flips ready false, rejects new commands, drains started commands, and quiesces Runtime Core once", async () => {
    let quiesceCalls = 0;
    const inFlight = deferred<string>("normal ACK");
    const lifecycle = new RuntimePodLifecycle({
      config: validConfig(),
      logger: createJsonLogger({ write: () => undefined }),
      bootstrap: successfulBootstrap(),
      runtimeProcess: fakeRuntimeProcess(),
      shutdownHooks: {
        quiesce: async () => {
          quiesceCalls++;
        },
      },
    });
    await lifecycle.start();
    const accepted = lifecycle.runCommand(async () => await inFlight.promise);

    const shutdown = lifecycle.shutdown();
    expect(lifecycle.ready()).toEqual({ ready: false });
    await expectNewCommandRejected(lifecycle);

    inFlight.resolve("normal ACK");
    await expect(accepted).resolves.toBe("normal ACK");
    await shutdown;

    expect(quiesceCalls).toBe(1);
  });

  test("metrics snapshot reports readiness, admission, and in-flight commands", async () => {
    const inFlight = deferred<string>("metrics ACK");
    const lifecycle = new RuntimePodLifecycle({
      config: validConfig(),
      logger: createJsonLogger({ write: () => undefined }),
      bootstrap: successfulBootstrap(),
      runtimeProcess: fakeRuntimeProcess(),
      shutdownHooks: idleCore(),
    });

    expect(lifecycle.metricsSnapshot()).toEqual({
      ready: false,
      accepting: false,
      inFlightCommands: 0,
    });

    await lifecycle.start();
    const accepted = lifecycle.runCommand(async () => await inFlight.promise);
    expect(lifecycle.metricsSnapshot()).toMatchObject({
      ready: true,
      accepting: true,
      inFlightCommands: 1,
    });

    inFlight.resolve("metrics ACK");
    await expect(accepted).resolves.toBe("metrics ACK");
    expect(lifecycle.metricsSnapshot().inFlightCommands).toBe(0);
    await lifecycle.shutdown();
  });

  test("shutdown drain timeout returns safe failure without cleanup, unbind, event writes, or raw details", async () => {
    const lifecycle = new RuntimePodLifecycle({
      config: shortShutdownConfig(),
      logger: createJsonLogger({ write: () => undefined }),
      bootstrap: successfulBootstrap(),
      runtimeProcess: fakeRuntimeProcess(),
      shutdownHooks: idleCore(),
    });
    await lifecycle.start();

    const owned = deferred<void>("owned command release");
    const blocked = lifecycle.runCommand(async () => await owned.promise);
    let closed = false;
    const shutdown = lifecycle.shutdown().then(() => {
      closed = true;
    });
    await expectGrpcCode(blocked, status.FAILED_PRECONDITION);
    expect(closed).toBe(false);
    owned.resolve(undefined);
    await shutdown;
  });

  test("shutdown active-run settlement rejection logs safe diagnostics without cleanup or unbind", async () => {
    const sink: string[] = [];
    const phaseSamples: Array<{phase:string;outcome:string;durationMs:number}> = [];
    const lifecycle = new RuntimePodLifecycle({
      observeShutdownPhase:(phase,durationMs,outcome)=>phaseSamples.push({phase,durationMs,outcome}),
      config: validConfig(),
      logger: createJsonLogger({ write: (line) => sink.push(line) }),
      bootstrap: successfulBootstrap(),
      runtimeProcess: fakeRuntimeProcess(),
      shutdownHooks: {
        quiesce: async () => {
          throw new Error("bearer token raw provider payload runtime-pod-a 10.0.0.1");
        },
      },
    });
    await lifecycle.start();

    await lifecycle.shutdown();
    expect(phaseSamples).toContainEqual(expect.objectContaining({phase:"shutdown_quiesce",outcome:"error"}));
    expect(phaseSamples.every(sample=>Number.isFinite(sample.durationMs) && sample.durationMs>=0)).toBe(true);

    const output = sink.join("\n");
    expect(output).toContain("shutdown_active_run_settlement_failed");
    expect(output).toContain("shutdown_error");
    expect(output).toContain("runtime pod shutdown active-run settlement failed");
    expect(output).toContain("error.message_safe");
    expect(output).toContain("error.code");
    for (const forbidden of [
      "bearer",
      "token",
      "raw provider payload",
      "runtime-pod-a",
      "10.0.0.1",
    ]) {
      expect(output).not.toContain(forbidden);
    }
  });

  test("one Session's rejected release does not stop another Session's handoff or close the client early", async () => {
    const records: Array<Record<string, unknown>> = [];
    const releaseB = deferred<void>("Session B release");
    const releases: string[] = [];
    let closed = false,
      closedBeforeB = false,
      bReleased = false;
    const lifecycle = new RuntimePodLifecycle({
      config: validConfig(),
      logger: {
        info: (record) => records.push(record),
        error: (record) => records.push(record),
      },
      bootstrap: successfulBootstrap(),
      runtimeProcess: {
        ...fakeRuntimeProcess(),
        release: async (request) => {
          releases.push(request.sessionId);
          if (request.sessionId === "sesn_a")
            throw new GrpcStatusError(
              status.FAILED_PRECONDITION,
              "binding is not checkpointed",
            );
          await releaseB.promise;
          bReleased = true;
          return {
            operationId: request.operationId,
            handoffId: "handoff_b",
            releasedBinding: {
              bindingId: request.bindingId,
              bindingGeneration: request.bindingGeneration,
              targetPodUid: "uid-a",
              runtimeProcessId: "process-test",
            },
            threads: [
              {
                sessionThreadId: "thrd_b",
                disposition:
                  RuntimeHandoffDisposition.RUNTIME_HANDOFF_DISPOSITION_RECOVER,
                queueJobId: "job_b",
              },
            ],
          };
        },
        close: async () => {
          closed = true;
          closedBeforeB = !bReleased;
        },
      },
      // Runtime Core releases each Session independently and settles after all of them.
      shutdownHooks: {
        quiesce: async (options) => {
          const outcomes = await Promise.allSettled(
            [sessionScope("sesn_a"), sessionScope("sesn_b")].map((scope) =>
              options.release(scope, options.settlementDeadline),
            ),
          );
          if (outcomes.some((outcome) => outcome.status === "rejected"))
            throw new Error("Runtime handoff incomplete");
        },
      },
    });
    await lifecycle.start();

    const shutdown = lifecycle.shutdown();
    await waitFor(
      () =>
        releases.length === 2 &&
        records.some(
          (record) => record.event === "runtime_binding_handoff_incomplete",
        ),
      "both Session releases and the rejected handoff record",
    );
    const incomplete = records.filter(
      (record) => record.event === "runtime_binding_handoff_incomplete",
    );
    expect(incomplete).toHaveLength(1);
    expect(incomplete[0]).toMatchObject({
      "session.id": "sesn_a",
      "binding.id": "bind_sesn_a",
      "binding.generation": 7,
      "grpc.code": "FailedPrecondition",
      operation: "shutdown",
    });
    expect(typeof incomplete[0]?.["operation.id"]).toBe("string");
    expect(closed).toBe(false);

    releaseB.resolve(undefined);
    await shutdown;

    expect(closed).toBe(true);
    expect(closedBeforeB).toBe(false);
    expect(
      records.filter(
        (record) => record.event === "runtime_binding_handoff_committed",
      ),
    ).toEqual([
      expect.objectContaining({
        "session.id": "sesn_b",
        "handoff.id": "handoff_b",
        "thread.id": "thrd_b",
      }),
    ]);
    expect(
      records.filter(
        (record) => record.event === "runtime_binding_handoff_incomplete",
      ),
    ).toHaveLength(1);
  });

  test("shutdown drain timeout aborts command leases before late handler mutation", async () => {
    const sink: string[] = [];
    const gate = deferred<void>("handler release");
    const lifecycle = new RuntimePodLifecycle({
      config: shortShutdownConfig(),
      logger: createJsonLogger({ write: (line) => sink.push(line) }),
      bootstrap: successfulBootstrap(),
      runtimeProcess: fakeRuntimeProcess(),
      shutdownHooks: idleCore(),
    });
    await lifecycle.start();

    const mutations: string[] = [];
    const command = lifecycle.runCommand(async (lease) => {
      const unregister = lease.onAbort(() => {
        mutations.push("rollback");
      });
      await gate.promise;
      unregister();
      lease.throwIfAborted();
      mutations.push("late-commit");
      return "ACK";
    });

    const shutdown = lifecycle.shutdown();
    await expectGrpcCode(command, status.FAILED_PRECONDITION);
    gate.resolve(undefined);
    await shutdown;
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(mutations).toEqual(["rollback"]);
    expect(sink.join("\n")).toContain("shutdown_drain_timeout");
    for (const forbidden of [
      "bearer",
      "token",
      "kubernetes.default.svc",
      "raw request body",
      "runtime-pod-a",
      "10.0.0.1",
    ]) {
      expect(sink.join("\n")).not.toContain(forbidden);
    }
  });
});

function validEnv() {
  return {
    TETRAL_RUNTIME_POD_NAMESPACE: "engine",
    TETRAL_RUNTIME_POD_NAME: "runtime-pod-a",
    TETRAL_RUNTIME_POD_UID: "uid-a",
    TETRAL_RUNTIME_POD_IP: "10.0.0.1",
    TETRAL_RUNTIME_POD_GRPC_PORT: "19090",
    TETRAL_RUNTIME_POD_HTTP_ADDR: "127.0.0.1:0",
    TETRAL_DEPLOYMENT_ENVIRONMENT: "test",
    TETRAL_SERVICE_VERSION: "test",
    TETRAL_RUNTIME_POD_GRPC_AUDIENCE: "tetral-internal-grpc",
    TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS: "engine/bridge",
    KUBERNETES_API_SERVER_URL: "https://kubernetes.default.svc",
    KUBERNETES_API_CA_CERT_PATH: "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
    KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH:
      "/var/run/secrets/kubernetes.io/serviceaccount/token",
    TETRAL_RUNTIME_POD_OUTBOUND_GRPC_TOKEN_PATH:
      "/var/run/secrets/tetral-internal-grpc/runtime-pod/token",
    TETRAL_BRIDGE_API_GRPC_ADDR: "bridge.engine.svc:9090",
    TETRAL_GATEWAY_GRPC_ADDR: "gateway.engine.svc:9090",
    TETRAL_MCP_CONNECTOR_GRPC_ADDR: "gateway.engine.svc:9091",
    TETRAL_WEB_CONNECTOR_GRPC_ADDR: "gateway.engine.svc:9092",
    TETRAL_RUNTIME_APPROVAL_REVIEWER_MODEL: "anthropic/claude-opus-4-8",
    TETRAL_RUNTIME_SKILL_GUIDANCE_DESCRIPTION_BUDGET_BYTES: "32768",
  };
}

function validConfig() {
  return loadRuntimePodConfig(validEnv());
}

/** Millisecond current-step, settlement and join windows expire held work deterministically. */
function shortShutdownConfig() {
  return loadRuntimePodConfig({
    ...validEnv(),
    TETRAL_RUNTIME_DRAIN_TIMEOUT_MS: "1",
    TETRAL_RUNTIME_SETTLEMENT_TIMEOUT_MS: "1",
    TETRAL_RUNTIME_LOCAL_JOIN_TIMEOUT_MS: "1",
  });
}

function fakeRuntimeProcess(): RuntimeProcessPort {
  return {
    runtimeProcessId: "process-test",
    register: async () => undefined,
    report: async () => undefined,
    release: async () => {
      throw new Error("unexpected release");
    },
    close: async () => undefined,
  };
}

function idleCore(): { readonly quiesce: (options: RuntimeQuiesceOptions) => Promise<void> } {
  return { quiesce: async () => undefined };
}

function sessionScope(sessionId: string) {
  return {
    workspaceId: "wksp_1",
    sessionId,
    sessionThreadId: `thrd_${sessionId}`,
    bindingId: `bind_${sessionId}`,
    bindingGeneration: 7,
    targetPodUid: "uid-a",
    runtimeProcessId: "process-test",
  };
}

async function waitFor(condition: () => boolean, label: string): Promise<void> {
  const deadline = Date.now() + 4_000;
  while (!condition()) {
    if (Date.now() >= deadline) throw new Error(`timed out waiting for ${label}`);
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
}

function successfulBootstrap() {
  return {
    runtime: async () => undefined,
    core: async () => undefined,
    grpc: async () => undefined,
    authClient: async () => undefined,
  };
}

function deferred<T>(valueLabel: string): {
  readonly promise: Promise<T>;
  readonly resolve: (value: T) => void;
} {
  let resolve: (value: T) => void = () => {
    throw new Error(`uninitialized ${valueLabel}`);
  };
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

async function expectNewCommandRejected(lifecycle: RuntimePodLifecycle): Promise<void> {
  try {
    await lifecycle.runCommand(async () => "new");
    throw new Error("new command accepted");
  } catch (error) {
    expect(error).toBeInstanceOf(GrpcStatusError);
    expect((error as GrpcStatusError).code).toBe(status.FAILED_PRECONDITION);
  }
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
