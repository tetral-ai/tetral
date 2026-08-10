import { describe, expect, test } from "bun:test";
import { createRuntimeGrpcServer } from "../../src/grpc-server.js";
import { createRuntimeHttpServer } from "../../src/http-server.js";
import { RuntimePodLifecycle } from "../../src/lifecycle.js";
import { RuntimePodMetricsRegistry } from "../../src/metrics.js";
import { RuntimeControlService } from "../../src/runtime-service.js";

describe("Runtime Pod server bind addresses", () => {
  test("HTTP server accepts explicit production host addresses, serves metrics, and rejects hostless addresses", async () => {
    const metricsRegistry = new RuntimePodMetricsRegistry();
    metricsRegistry.recordHotState({ activeSessions: 2, activeThreads: 3, activeFibers: 1, pendingApprovals: 1 });
    metricsRegistry.addActiveToolFibers(2);
    metricsRegistry.observeProviderStreamDuration("agent_provider_request", 42, "success");
    metricsRegistry.observeEventWriteLatency("append", 7, "success");
    metricsRegistry.observeContextLoadLatency("commit_accepted_input", 11, "success");
    metricsRegistry.recordCleanupCommandOutcome("completed");
    metricsRegistry.recordReceiptEvidence("stale_custody");
    metricsRegistry.recordCloseoutEvent({ event: "runtime_closeout_stalled", activeCloseouts: 2 });
    metricsRegistry.recordCloseoutEvent({ event: "runtime_closeout_recovered", activeCloseouts: 0 });
    const httpServer = createRuntimeHttpServer("0.0.0.0:0", fakeLifecycle(), metricsRegistry);
    try {
      expect(httpServer.url.port).not.toBe("");
      const metrics = await fetch(new URL("/metrics", httpServer.url));
      expect(metrics.status).toBe(200);
      expect(metrics.headers.get("content-type")).toContain("text/plain");
      const body = await metrics.text();
      expect(body).toContain("runtimepod_ready 0");
      expect(body).toContain("runtimepod_commands_in_flight");
      expect(body).toContain("runtimepod_active_sessions 2");
      expect(body).toContain("runtimepod_active_threads 3");
      expect(body).toContain("runtimepod_active_fibers 1");
      expect(body).toContain("runtimepod_active_tool_fibers 2");
      expect(body).toContain("runtimepod_pending_approvals 1");
      expect(body).toContain('runtimepod_provider_stream_duration_ms_count{kind="agent_provider_request",outcome="success"} 1');
      expect(body).toContain('runtimepod_event_write_latency_ms_count{operation="append",outcome="success"} 1');
      expect(body).toContain('runtimepod_context_load_latency_ms_count{operation="commit_accepted_input",outcome="success"} 1');
      expect(body).toContain('runtimepod_cleanup_command_outcomes_total{outcome="completed"} 1');
      expect(body).toContain('runtimepod_receipt_evidence_total{outcome="stale_custody"} 1');
      expect(body).toContain('runtimepod_closeout_events_total{event="runtime_closeout_stalled"} 2');
      expect(body).toContain('runtimepod_closeout_events_total{event="runtime_closeout_recovered"} 1');
    } finally {
      httpServer.stop();
    }

    expect(() => createRuntimeHttpServer(":0", fakeLifecycle())).toThrow("invalid http bind address");
  });

  test("gRPC server binds explicit host addresses for ephemeral ports", async () => {
    const grpcServer = createRuntimeGrpcServer(fakeRuntimeControlService());
    try {
      const port = await grpcServer.bind("127.0.0.1:0");
      expect(port).toBeGreaterThan(0);
    } finally {
      await grpcServer.shutdown();
    }
  });
});

function fakeLifecycle() {
  return new RuntimePodLifecycle({
    config: {
      ok: true,
      config: {
        ownPod: { namespace: "engine", name: "runtime-pod-a", uid: "uid-a", ip: "10.0.0.1" },
        deploymentEnvironment: "test",
        serviceVersion: "test",
        bridge: { namespace: "engine", serviceAccount: "bridge" },
        grpcBindAddress: "127.0.0.1:0",
        httpBindAddress: "127.0.0.1:0",
        kubernetesApiServerUrl: "https://kubernetes.default.svc",
        kubernetesApiCaCertPath: "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
        tokenReviewReviewerTokenPath: "/var/run/secrets/kubernetes.io/serviceaccount/token",
        outboundInternalGrpcTokenPath: "/var/run/secrets/tetral-internal-grpc/runtime-pod/token",
        bridgeApiGrpcAddress: "bridge.engine.svc:9090",
        gatewayGrpcAddress: "gateway.engine.svc:9090",
        mcpConnectorGrpcAddress: "gateway.engine.svc:9091",
        webConnectorGrpcAddress: "gateway.engine.svc:9092",
        providerStreamTimeoutMs: 1_800_000,
        platformModels: {
          approvalReviewer: { providerId: "anthropic", modelId: "claude-opus-4-8" },
        },
        skillGuidance: {
          descriptionBudgetBytes: 32_768,
        },
      },
    },
    logger: { info: () => undefined, error: () => undefined },
    bootstrap: {
      runtime: async () => undefined,
      core: async () => undefined,
      grpc: async () => undefined,
      authClient: async () => undefined,
    },
  });
}

function fakeRuntimeControlService(): RuntimeControlService {
  return new RuntimeControlService({
    ownPod: { namespace: "engine", name: "runtime-pod-a", uid: "uid-a", ip: "10.0.0.1" },
    allowedBridge: { namespace: "engine", name: "bridge" },
    authenticator: {
      authenticate: async () => ({ ok: true, serviceAccount: { namespace: "engine", name: "bridge" } }),
    },
    runHost: {
      handleAcceptInput: async (command) => ({ ok: true, sessionId: command.sessionId, created: false, started: false }),
      handleAgentMail: async (command) => ({ ok: true, sessionId: command.sessionId, applied: true }),
      handleInterruptControl: async (sessionId) => ({ ok: true, sessionId, created: false, interrupted: true, idleInterrupt: false }),
      handleToolConfirmation: async (sessionId) => ({ ok: true, sessionId, created: false, applied: true }),
      handleTaskNotification: async (sessionId) => ({ ok: true, sessionId, created: false, applied: true }),
      handleRuntimeConfigPatch: async (sessionId) => ({ ok: true, sessionId, created: false, applied: true }),
    },
    cleanupController: {
      startCleanup: async (scope) => ({
        ok: true,
        sessionId: scope.sessionId,
        completion: Promise.resolve({ ok: true, sessionId: scope.sessionId, cleaned: true }),
      }),
    },
    logger: { info: () => undefined, error: () => undefined },
    ready: () => true,
  });
}
