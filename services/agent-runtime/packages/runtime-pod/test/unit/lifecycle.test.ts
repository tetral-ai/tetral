import { describe, expect, test } from "bun:test";
import { status } from "@grpc/grpc-js";
import { loadRuntimePodConfig } from "../../src/config.js";
import { createJsonLogger } from "../../src/logger.js";
import { GrpcStatusError, RuntimePodLifecycle } from "../../src/lifecycle.js";

describe("Runtime Pod lifecycle", () => {
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
    });

    expect(lifecycle.health()).toEqual({ ok: true });
    expect(lifecycle.ready()).toEqual({ ready: false });

    await lifecycle.start();

    expect(lifecycle.ready()).toEqual({ ready: true });
  });

  test("config/env failure is classified as config_error and readiness remains false", async () => {
    const sink: string[] = [];
    const parsed = loadRuntimePodConfig({ ...validEnv(), TETRAL_RUNTIME_POD_IP: "runtime.service.local" });

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

    expect(loadRuntimePodConfig({ ...validEnv(), TETRAL_RUNTIME_APPROVAL_REVIEWER_MODEL: "anthropic/" }).ok).toBe(false);
    expect(loadRuntimePodConfig({ ...validEnv(), TETRAL_RUNTIME_SKILL_GUIDANCE_DESCRIPTION_BUDGET_BYTES: "65536" }).ok).toBe(false);
    expect(loadRuntimePodConfig({ ...validEnv(), TETRAL_RUNTIME_PROVIDER_STREAM_TIMEOUT_MS: "0" }).ok).toBe(false);
    expect(loadRuntimePodConfig({ ...validEnv(), TETRAL_RUNTIME_PROVIDER_STREAM_TIMEOUT_MS: "1.5" }).ok).toBe(false);
    expect(loadRuntimePodConfig({ ...validEnv(), TETRAL_RUNTIME_PROVIDER_STREAM_TIMEOUT_MS: "2147483647" }).ok).toBe(true);
    expect(loadRuntimePodConfig({ ...validEnv(), TETRAL_RUNTIME_PROVIDER_STREAM_TIMEOUT_MS: "2147483648" }).ok).toBe(false);
  });

  test("dependency, listener, and auth-client failures are startup_error without raw details", async () => {
    for (const scenario of [
      {
        name: "dependency",
        bootstrap: { ...successfulBootstrap(), core: async () => { throw new Error("postgres://secret@host/db raw provider payload sk-provider-key"); } },
      },
      {
        name: "listener",
        bootstrap: { ...successfulBootstrap(), grpc: async () => { throw new Error("127.0.0.1:19090 bind failed raw request body"); } },
      },
      {
        name: "auth",
        bootstrap: {
          ...successfulBootstrap(),
          authClient: async () => {
            throw new Error(`bearer secret-token https://kubernetes.default.svc {"kind":"TokenReview","status":{"error":"kube object dump"}}`);
          },
        },
      },
    ]) {
      const sink: string[] = [];
      const lifecycle = new RuntimePodLifecycle({
        config: validConfig(),
        logger: createJsonLogger({ write: (line) => sink.push(line) }),
        bootstrap: scenario.bootstrap,
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

  test("shutdown flips ready false, rejects new commands, drains started commands, and only settles hot active runs", async () => {
    let shutdownActiveRunCalls = 0;
    const inFlight = deferred("normal ACK");
    const lifecycle = new RuntimePodLifecycle({
      config: validConfig(),
      logger: createJsonLogger({ write: () => undefined }),
      bootstrap: successfulBootstrap(),
      shutdownHooks: {
        shutdownActiveRuns: async () => { shutdownActiveRunCalls++; },
      },
      drainTimeoutMs: 50,
    });
    await lifecycle.start();
    const accepted = lifecycle.trackCommand(inFlight.promise);

    const shutdown = lifecycle.shutdown();
    expect(lifecycle.ready()).toEqual({ ready: false });
    await expectNewCommandRejected(lifecycle);

    inFlight.resolve("normal ACK");
    await expect(accepted).resolves.toBe("normal ACK");
    await shutdown;

    expect(shutdownActiveRunCalls).toBe(1);
  });

  test("metrics snapshot reports readiness, admission, and in-flight commands", async () => {
    const inFlight = deferred("metrics ACK");
    const lifecycle = new RuntimePodLifecycle({
      config: validConfig(),
      logger: createJsonLogger({ write: () => undefined }),
      bootstrap: successfulBootstrap(),
    });

    expect(lifecycle.metricsSnapshot()).toEqual({
      ready: false,
      accepting: false,
      inFlightCommands: 0,
    });

    await lifecycle.start();
    const accepted = lifecycle.trackCommand(inFlight.promise);
    expect(lifecycle.metricsSnapshot()).toMatchObject({
      ready: true,
      accepting: true,
      inFlightCommands: 1,
    });

    inFlight.resolve("metrics ACK");
    await expect(accepted).resolves.toBe("metrics ACK");
    expect(lifecycle.metricsSnapshot().inFlightCommands).toBe(0);
  });

  test("shutdown drain timeout returns safe failure without cleanup, unbind, event writes, or raw details", async () => {
    const lifecycle = new RuntimePodLifecycle({
      config: validConfig(),
      logger: createJsonLogger({ write: () => undefined }),
      bootstrap: successfulBootstrap(),
      shutdownHooks: {
        shutdownActiveRuns: async () => undefined,
      },
      drainTimeoutMs: 1,
    });
    await lifecycle.start();

    const blocked = lifecycle.trackCommand(new Promise(() => undefined));
    await lifecycle.shutdown();

    await expectGrpcCode(blocked, status.FAILED_PRECONDITION);
  });

  test("shutdown active-run settlement rejection logs safe diagnostics without cleanup or unbind", async () => {
    const sink: string[] = [];
    const lifecycle = new RuntimePodLifecycle({
      config: validConfig(),
      logger: createJsonLogger({ write: (line) => sink.push(line) }),
      bootstrap: successfulBootstrap(),
      shutdownHooks: {
        shutdownActiveRuns: async () => {
          throw new Error("bearer token raw provider payload runtime-pod-a 10.0.0.1");
        },
      },
      drainTimeoutMs: 50,
    });
    await lifecycle.start();

    await lifecycle.shutdown();

    const output = sink.join("\n");
    expect(output).toContain("shutdown_active_run_settlement_failed");
    expect(output).toContain("shutdown_error");
    expect(output).toContain("runtime pod shutdown active-run settlement failed");
    expect(output).toContain("error.message_safe");
    expect(output).toContain("error.code");
    for (const forbidden of ["bearer", "token", "raw provider payload", "runtime-pod-a", "10.0.0.1"]) {
      expect(output).not.toContain(forbidden);
    }
  });

  test("shutdown drain timeout aborts command leases before late handler mutation", async () => {
    const sink: string[] = [];
    const gate = deferred<void>("handler release");
    const lifecycle = new RuntimePodLifecycle({
      config: validConfig(),
      logger: createJsonLogger({ write: (line) => sink.push(line) }),
      bootstrap: successfulBootstrap(),
      drainTimeoutMs: 1,
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

    await lifecycle.shutdown();
    await expectGrpcCode(command, status.FAILED_PRECONDITION);
    gate.resolve(undefined);
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(mutations).toEqual(["rollback"]);
    expect(sink.join("\n")).toContain("shutdown_drain_timeout");
    for (const forbidden of ["bearer", "token", "kubernetes.default.svc", "raw request body", "runtime-pod-a", "10.0.0.1"]) {
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
    TETRAL_RUNTIME_POD_GRPC_PORT: "9090",
    TETRAL_RUNTIME_POD_HTTP_ADDR: "127.0.0.1:0",
    TETRAL_DEPLOYMENT_ENVIRONMENT: "test",
    TETRAL_SERVICE_VERSION: "test",
    TETRAL_RUNTIME_POD_GRPC_AUDIENCE: "tetral-internal-grpc",
    TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS: "engine/bridge",
    KUBERNETES_API_SERVER_URL: "https://kubernetes.default.svc",
    KUBERNETES_API_CA_CERT_PATH: "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
    KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH: "/var/run/secrets/kubernetes.io/serviceaccount/token",
    TETRAL_RUNTIME_POD_OUTBOUND_GRPC_TOKEN_PATH: "/var/run/secrets/tetral-internal-grpc/runtime-pod/token",
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

function successfulBootstrap() {
  return {
    runtime: async () => undefined,
    core: async () => undefined,
    grpc: async () => undefined,
    authClient: async () => undefined,
  };
}

function deferred<T>(valueLabel: string): { readonly promise: Promise<T>; readonly resolve: (value: T) => void } {
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
    await lifecycle.trackCommand(Promise.resolve("new"));
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
