/** Controlled dependencies exercise the production Runtime application and RPC
 * fences. This local fixture does not exercise Kubernetes TokenReview itself. */
import { readFile } from "node:fs/promises";
import { createRuntimePodApp } from "../../src/app.js";
import { hardenedRoutingSecretsReady } from "../../src/routing-proxy.js";
import { DefaultBridgeMethodPolicies } from "../../src/bridge-policy.js";
import type { RuntimePodConfig } from "../../src/config.js";
import type { RuntimeSessionRunHost } from "../../src/runtime-service.js";

const input = JSON.parse(await readFile(process.argv[2]!, "utf8")) as {
  profile: "standard-routed" | "hardened"; podUid: string; processId: string;
};
let operations = 0, held = false, release: (() => void) | undefined;
let registrations=0,acceptingReports=0,processClosed=false;
const host: RuntimeSessionRunHost = {
  handleAcceptInput: async (command) => {
    operations++;
    if (held) await new Promise<void>((resolve) => { release = resolve; });
    return { ok: true, sessionId: command.sessionId, created: true, started: true };
  },
  handleAgentMail: async (command) => ({ ok: true, sessionId: command.sessionId, applied: true }),
  handleTaskNotification: async (sessionId) => ({ ok: true, sessionId, created: false, applied: true }),
  handleInterruptControl: async (sessionId) => ({ ok: true, sessionId, created: false, interrupted: true, idleInterrupt: false }),
  handleToolConfirmation: async (sessionId) => ({ ok: true, sessionId, created: false, applied: true }),
  handleRuntimeConfigPatch: async (sessionId) => ({ ok: true, sessionId, created: false, applied: true }),
};
const config: RuntimePodConfig = {
  ownPod: { namespace: "tetral-agent-runtime", name: input.podUid, uid: input.podUid, ip: "127.0.0.1" },
  deploymentEnvironment: "test", diagnostics: { level: "info", maxRecordBytes: 16384, summaryIntervalMs: 30000, burst: 1 },
  serviceVersion: "test", jobRunner: { namespace: "tetral-system", serviceAccount: "job-runner" },
  grpcBindAddress: input.profile === "hardened" ? "127.0.0.1:9090" : "0.0.0.0:19090", httpBindAddress: "127.0.0.1:8080",
  kubernetesApiServerUrl: "https://kubernetes.default.svc", kubernetesApiCaCertPath: "/unused",
  tokenReviewReviewerTokenPath: "/unused", outboundInternalGrpcTokenPath: "/unused",
  bridgeApiGrpcAddress: "bridge.tetral-system.svc.cluster.local:9090", gatewayGrpcAddress: "provider-gateway.tetral-system.svc.cluster.local:9090",
  mcpConnectorGrpcAddress: "mcp-connector.tetral-system.svc.cluster.local:9091", webConnectorGrpcAddress: "web-connector.tetral-system.svc.cluster.local:9092",
  providerStreamTimeoutMs: 1800000, bridgeMethodPolicies: DefaultBridgeMethodPolicies,
  transportProfile: input.profile, maxLocalSessions: 256, maxConcurrentTools: 8,
  lifecycle: { reportIntervalMs: 2000, processFreshnessMs: 10000, currentStepTimeoutMs: 60000, settlementTimeoutMs: 15000, settlementAttemptTimeoutMs: 5000, localJoinTimeoutMs: 5000, proxyJoinTimeoutMs: 5000 },
  platformModels: { approvalReviewer: { providerId: "deepseek", modelId: "deepseek-v4-pro" } },
  skillGuidance: { descriptionBudgetBytes: 32768 },
};
const app = createRuntimePodApp({
  config, runtimeProcessId: input.processId,
  runtimeProcess: { runtimeProcessId: input.processId,
    register: async () => { registrations++; },
    report: async (phase) => { if(registrations!==1) throw new Error("process is not registered"); if(phase==="accepting") acceptingReports++; },
    release: async () => { throw new Error("fixture owns no handoff receipt"); },
    close: async () => { processClosed=true; },
  },
  logger: { info: () => undefined, error: () => undefined },
  tokenReviewClient: { createTokenReview: async ({ token }) => ({ authenticated: token === "fixture-runner", audiences: ["tetral-internal-grpc"], username: "system:serviceaccount:tetral-system:job-runner" }) },
  commandRunHost: host,
  cleanupRunHost: { handleCleanupSession: async (scope) => ({ ok: true, sessionId: scope.sessionId, cleaned: true }) },
  bootstrap: { runtime: async () => {
    const deadline = Date.now() + 3000;
    while (Date.now() < deadline) {
      try {
        const readiness = await fetch("http://127.0.0.1:9901/ready", { signal: AbortSignal.timeout(500) });
        await readiness.body?.cancel();
        if (readiness.ok) {
          if (input.profile !== "hardened") return;
          const secrets = await fetch("http://127.0.0.1:9901/stats?format=json&filter=^sds\\.tetral\\.runtime\\.direct_(leaf|validation)\\.update_success$", { signal: AbortSignal.timeout(500) });
          if (secrets.ok && hardenedRoutingSecretsReady(await secrets.json())) return;
        }
      } catch { /* bounded startup observation */ }
      await Bun.sleep(100);
    }
    throw new Error("mandatory local proxy is unavailable");
  } },
});
let ready = false;
const control = Bun.serve({ hostname: "0.0.0.0", port: 8888, fetch: async (request) => {
  const path = new URL(request.url).pathname;
  if (path === "/operations") return Response.json({ operations, ready, held, registrations, acceptingReports, processClosed });
  if (path === "/hold") { held = true; return new Response("ok"); }
  if (path === "/release") { held = false; release?.(); release = undefined; return new Response("ok"); }
  if (path === "/stop") {
    ready = false; held = false; release?.();
    await app.shutdown();
    setTimeout(() => { control.stop(true); process.exit(0); }, 0);
    return new Response("joined");
  }
  return new Response("unknown", { status: 404 });
} });
try { await app.start(); if(registrations!==1||acceptingReports<1) throw new Error("startup registration/report acknowledgment absent"); ready = true; }
catch { control.stop(true); process.exitCode = 1; }
