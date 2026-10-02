import { expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { Server, ServerCredentials, status } from "@grpc/grpc-js";
import {
  AgentRuntimeBridgeServiceService,
  RuntimeProcessPhase,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type { AgentRuntimeBridgeServiceServer } from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";

type ReadinessOutcome =
  | {
      readonly kind: "ready";
      readonly status: 200;
      readonly body: { readonly ready: true };
    }
  | { readonly kind: "exited"; readonly exitCode: number }
  | { readonly kind: "deadline" }
  | {
      readonly kind: "unexpected";
      readonly status: number;
      readonly body: string;
    };

test("production entrypoint reaches positive HTTP readiness", async () => {
  const fixtureDir = await mkdtemp(join(tmpdir(), "runtime-entrypoint-"));
  const reviewerTokenPath = join(fixtureDir, "reviewer-token");
  const caCertPath = join(fixtureDir, "ca.crt");
  const outboundTokenPath = join(fixtureDir, "outbound-token");
  await writeFile(reviewerTokenPath, "reviewer-token\n", { mode: 0o600 });
  await writeFile(caCertPath, "test-ca-material\n", { mode: 0o600 });
  await writeFile(outboundTokenPath, "outbound-token\n", { mode: 0o600 });

  // Production binds the exact standard-profile Runtime port. Only the
  // operational HTTP listener uses an ephemeral fixture port.
  const httpPort = await reserveLoopbackPort();
  const dependencies = await startEntrypointDependencies();
  const child = Bun.spawn({
    cmd: [process.execPath, resolve(import.meta.dir, "../../src/command.ts")],
    cwd: resolve(import.meta.dir, "../../../.."),
    env: {
      TETRAL_RUNTIME_POD_NAMESPACE: "engine",
      TETRAL_RUNTIME_POD_NAME: "runtime-pod-entrypoint-test",
      TETRAL_RUNTIME_POD_UID: "uid-entrypoint-test",
      TETRAL_RUNTIME_POD_IP: "127.0.0.1",
      TETRAL_RUNTIME_POD_GRPC_PORT: "19090",
      TETRAL_TRANSPORT_PROFILE: "standard-routed",
      TETRAL_ROUTING_PROXY_REQUIRED: "true",
      TETRAL_RUNTIME_POD_HTTP_ADDR: `127.0.0.1:${httpPort}`,
      TETRAL_DEPLOYMENT_ENVIRONMENT: "test",
      TETRAL_SERVICE_VERSION: "test",
      TETRAL_RUNTIME_POD_GRPC_AUDIENCE: "tetral-internal-grpc",
      TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS: "engine/job-runner",
      KUBERNETES_API_SERVER_URL: "https://kubernetes.default.svc",
      KUBERNETES_API_CA_CERT_PATH: caCertPath,
      KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH: reviewerTokenPath,
      TETRAL_RUNTIME_POD_OUTBOUND_GRPC_TOKEN_PATH: outboundTokenPath,
      TETRAL_BRIDGE_API_GRPC_ADDR: dependencies.bridgeAddress,
      TETRAL_GATEWAY_GRPC_ADDR: "gateway.engine.svc:9090",
      TETRAL_MCP_CONNECTOR_GRPC_ADDR: "gateway.engine.svc:9091",
      TETRAL_WEB_CONNECTOR_GRPC_ADDR: "gateway.engine.svc:9092",
      TETRAL_RUNTIME_APPROVAL_REVIEWER_MODEL: "anthropic/claude-opus-4-8",
      TETRAL_RUNTIME_SKILL_GUIDANCE_DESCRIPTION_BUDGET_BYTES: "32768",
    },
    stdout: "pipe",
    stderr: "pipe",
  });
  const stdoutPromise = new Response(child.stdout).text();
  const stderrPromise = new Response(child.stderr).text();

  let outcome: ReadinessOutcome;
  try {
    await dependencies.waitForAcceptingReport();
    const beforeAccepting = await fetch(`http://127.0.0.1:${httpPort}/ready`, {
      signal: AbortSignal.timeout(500),
    });
    expect(beforeAccepting.status).toBe(503);
    expect(await beforeAccepting.json()).toEqual({ ready: false });
    dependencies.acknowledgeAccepting();
    outcome = await waitForReadiness(
      child,
      `http://127.0.0.1:${httpPort}/ready`,
      10_000,
      20,
    );
  } catch {
    outcome = child.exitCode === null
      ? { kind: "deadline" }
      : { kind: "exited", exitCode: child.exitCode };
  } finally {
    dependencies.acknowledgeAccepting();
    try {
      await terminateChild(child, 5_000);
    } finally {
      await dependencies.stop();
      await rm(fixtureDir, { recursive: true, force: true });
    }
  }
  const [stdout, stderr] = await Promise.all([stdoutPromise, stderrPromise]);
  const diagnostic = `entrypoint readiness outcome=${JSON.stringify(outcome)} stdout=${stdout.slice(0, 2_000)} stderr=${stderr.slice(0, 2_000)}`;
  expect(outcome, diagnostic).toEqual({
    kind: "ready",
    status: 200,
    body: { ready: true },
  });
  expect(dependencies.registrationCount()).toBe(1);
  expect(dependencies.phases()).toEqual([
    RuntimeProcessPhase.RUNTIME_PROCESS_PHASE_ACCEPTING,
    RuntimeProcessPhase.RUNTIME_PROCESS_PHASE_DRAINING,
  ]);
}, 30_000);

async function reserveLoopbackPort(): Promise<number> {
  const listener = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch: () => new Response("reserved"),
  });
  const port = listener.port;
  await listener.stop(true);
  if (port === undefined)
    throw new Error("Bun did not assign a concrete loopback port");
  return port;
}

/** Real loopback dependencies for the unmodified production command entrypoint. */
async function startEntrypointDependencies() {
  const server = new Server();
  let runtimeProcessId: string | undefined;
  let registrations = 0;
  const reportedPhases: RuntimeProcessPhase[] = [];
  let acceptingReceived!: () => void;
  const acceptingReport = new Promise<void>((resolve) => {
    acceptingReceived = resolve;
  });
  let acceptAcknowledgement: (() => void) | undefined;
  const implementation: Pick<
    AgentRuntimeBridgeServiceServer,
    "registerRuntimeProcess" | "reportRuntimeProcess"
  > = {
    registerRuntimeProcess(call, callback) {
      if (
        call.metadata.get("authorization")[0] !== "bearer outbound-token" ||
        !call.request.runtimeProcessId ||
        (runtimeProcessId !== undefined &&
          runtimeProcessId !== call.request.runtimeProcessId)
      ) {
        callback({
          code: status.FAILED_PRECONDITION,
          message: "invalid fixture registration",
        });
        return;
      }
      runtimeProcessId = call.request.runtimeProcessId;
      registrations += 1;
      callback(null, {
        runtimeProcessId,
        registrationOrder: 1,
        registrationReceipt: "entrypoint-registration-receipt",
      });
    },
    reportRuntimeProcess(call, callback) {
      if (
        call.metadata.get("authorization")[0] !== "bearer outbound-token" ||
        call.request.runtimeProcessId !== runtimeProcessId ||
        call.request.registrationReceipt !==
          "entrypoint-registration-receipt" ||
        ![
          RuntimeProcessPhase.RUNTIME_PROCESS_PHASE_ACCEPTING,
          RuntimeProcessPhase.RUNTIME_PROCESS_PHASE_DRAINING,
        ].includes(call.request.phase)
      ) {
        callback({
          code: status.FAILED_PRECONDITION,
          message: "invalid fixture report",
        });
        return;
      }
      reportedPhases.push(call.request.phase);
      const acknowledge = () =>
        callback(null, {
          runtimeProcessId: call.request.runtimeProcessId,
          phase: call.request.phase,
          current: true,
        });
      if (reportedPhases.length === 1) {
        acceptAcknowledgement = acknowledge;
        acceptingReceived();
      } else acknowledge();
    },
  };
  server.addService(AgentRuntimeBridgeServiceService, implementation);
  const port = await new Promise<number>((resolve, reject) => {
    server.bindAsync(
      "127.0.0.1:0",
      ServerCredentials.createInsecure(),
      (error, boundPort) => {
        if (error !== null) reject(error);
        else resolve(boundPort);
      },
    );
  });
  let proxy;
  try {
    proxy = Bun.serve({
      hostname: "127.0.0.1",
      port: 15021,
      fetch: (request) =>
        new Response(null, {
          status:
            new URL(request.url).pathname === "/healthz/ready" ? 200 : 404,
        }),
    });
  } catch (error) {
    await new Promise<void>((resolve) => server.tryShutdown(() => resolve()));
    throw error;
  }
  return {
    bridgeAddress: `127.0.0.1:${port}`,
    acknowledgeAccepting: () => {
      const acknowledge = acceptAcknowledgement;
      acceptAcknowledgement = undefined;
      acknowledge?.();
    },
    waitForAcceptingReport: async () => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        await Promise.race([
          acceptingReport,
          new Promise<never>((_resolve, reject) => {
            timer = setTimeout(
              () =>
                reject(new Error("entrypoint accepting report not received")),
              5_000,
            );
          }),
        ]);
      } finally {
        if (timer !== undefined) clearTimeout(timer);
      }
    },
    registrationCount: () => registrations,
    phases: () => reportedPhases,
    stop: async () => {
      await proxy.stop(true);
      await new Promise<void>((resolve) => server.tryShutdown(() => resolve()));
    },
  };
}

async function waitForReadiness(
  child: { readonly exitCode: number | null },
  url: string,
  timeoutMs: number,
  intervalMs: number,
): Promise<ReadinessOutcome> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (child.exitCode !== null) {
      return { kind: "exited", exitCode: child.exitCode };
    }
    try {
      const response = await fetch(url, { signal: AbortSignal.timeout(500) });
      const body = await response.text();
      if (response.status === 200) {
        const parsed = JSON.parse(body) as unknown;
        if (isReadyResponse(parsed)) {
          return { kind: "ready", status: 200, body: parsed };
        }
        return { kind: "unexpected", status: response.status, body };
      }
      if (response.status !== 503 || body !== '{"ready":false}') {
        return { kind: "unexpected", status: response.status, body };
      }
    } catch {
      // Connection refusal and short per-request timeouts are retryable until
      // the named readiness deadline.
    }
    await Bun.sleep(intervalMs);
  }
  return { kind: "deadline" };
}

function isReadyResponse(value: unknown): value is { readonly ready: true } {
  return (
    typeof value === "object" &&
    value !== null &&
    "ready" in value &&
    value.ready === true
  );
}

async function terminateChild(
  child: {
    readonly exitCode: number | null;
    readonly exited: Promise<number>;
    kill(signal?: number | NodeJS.Signals): void;
  },
  timeoutMs: number,
): Promise<void> {
  if (child.exitCode !== null) {
    await child.exited;
    return;
  }
  child.kill("SIGTERM");
  const exited = await Promise.race([
    child.exited.then(() => true),
    Bun.sleep(timeoutMs).then(() => false),
  ]);
  if (exited) {
    return;
  }
  child.kill("SIGKILL");
  await child.exited;
}
