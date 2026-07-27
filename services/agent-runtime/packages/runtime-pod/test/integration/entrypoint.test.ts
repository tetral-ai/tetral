import { expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

type ReadinessOutcome =
  | { readonly kind: "ready"; readonly status: 200; readonly body: { readonly ready: true } }
  | { readonly kind: "exited"; readonly exitCode: number }
  | { readonly kind: "deadline" }
  | { readonly kind: "unexpected"; readonly status: number; readonly body: string };

test("production entrypoint reaches positive HTTP readiness", async () => {
  const fixtureDir = await mkdtemp(join(tmpdir(), "runtime-entrypoint-"));
  const reviewerTokenPath = join(fixtureDir, "reviewer-token");
  const caCertPath = join(fixtureDir, "ca.crt");
  const outboundTokenPath = join(fixtureDir, "outbound-token");
  await writeFile(reviewerTokenPath, "reviewer-token\n", { mode: 0o600 });
  await writeFile(caCertPath, "test-ca-material\n", { mode: 0o600 });
  await writeFile(outboundTokenPath, "outbound-token\n", { mode: 0o600 });

  // Releasing two loopback listeners before spawn leaves an unavoidable TOCTOU
  // window, but gives the child concrete ports whose readiness can be probed.
  const [grpcPort, httpPort] = await reserveLoopbackPorts();
  const child = Bun.spawn({
    cmd: [process.execPath, resolve(import.meta.dir, "../../src/command.ts")],
    cwd: resolve(import.meta.dir, "../../../.."),
    env: {
      TETRAL_RUNTIME_POD_NAMESPACE: "engine",
      TETRAL_RUNTIME_POD_NAME: "runtime-pod-entrypoint-test",
      TETRAL_RUNTIME_POD_UID: "uid-entrypoint-test",
      TETRAL_RUNTIME_POD_IP: "127.0.0.1",
      TETRAL_RUNTIME_POD_GRPC_PORT: String(grpcPort),
      TETRAL_RUNTIME_POD_HTTP_ADDR: `127.0.0.1:${httpPort}`,
      TETRAL_DEPLOYMENT_ENVIRONMENT: "test",
      TETRAL_SERVICE_VERSION: "test",
      TETRAL_RUNTIME_POD_GRPC_AUDIENCE: "tetral-internal-grpc",
      TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS: "engine/bridge",
      KUBERNETES_API_SERVER_URL: "https://kubernetes.default.svc",
      KUBERNETES_API_CA_CERT_PATH: caCertPath,
      KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH: reviewerTokenPath,
      TETRAL_RUNTIME_POD_OUTBOUND_GRPC_TOKEN_PATH: outboundTokenPath,
      TETRAL_BRIDGE_API_GRPC_ADDR: "bridge.engine.svc:9090",
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
    outcome = await waitForReadiness(child, `http://127.0.0.1:${httpPort}/ready`, 10_000, 20);
  } finally {
    await terminateChild(child, 5_000);
    await rm(fixtureDir, { recursive: true, force: true });
  }
  const [stdout, stderr] = await Promise.all([stdoutPromise, stderrPromise]);
  const diagnostic = `entrypoint readiness outcome=${JSON.stringify(outcome)} stdout=${stdout.slice(0, 2_000)} stderr=${stderr.slice(0, 2_000)}`;
  expect(outcome, diagnostic).toEqual({ kind: "ready", status: 200, body: { ready: true } });
}, 30_000);

async function reserveLoopbackPorts(): Promise<readonly [number, number]> {
  const first = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: () => new Response("reserved") });
  const second = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: () => new Response("reserved") });
  const firstPort = first.port;
  const secondPort = second.port;
  await Promise.all([first.stop(true), second.stop(true)]);
  if (firstPort === undefined || secondPort === undefined) {
    throw new Error("Bun did not assign concrete loopback ports");
  }
  return [firstPort, secondPort];
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
  return typeof value === "object" && value !== null && "ready" in value && value.ready === true;
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
