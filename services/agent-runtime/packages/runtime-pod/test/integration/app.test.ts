import { describe, expect, test } from "bun:test";
import { credentials, Metadata, status } from "@grpc/grpc-js";
import {
  AgentRuntimePodServiceClient,
  RuntimeCommandKind,
  RuntimeCommandStatus,
} from "@tetral/agent-runtime-protocol/src/gen/tetral/agent_runtime/v1/agent_runtime.js";
import type {
  RuntimeInputCommandRequest,
  RuntimeInputCommandResponse,
} from "@tetral/agent-runtime-protocol/src/gen/tetral/agent_runtime/v1/agent_runtime.js";
import { createRuntimePodApp } from "../../src/app.js";
import type { RuntimeTokenReviewClient } from "../../src/auth.js";
import type { RuntimePodConfig } from "../../src/config.js";
import type { RuntimeSessionRunHost } from "../../src/runtime-service.js";
import type { RuntimeCoreCleanupHost } from "../../src/cleanup-controller.js";

describe("RuntimePodApp production composition", () => {
  test("serves HTTP health/ready probes from lifecycle state while a command is in flight", async () => {
    const acceptGate = deferred<void>();
    const runHost = new RecordingRunHost(acceptGate.promise);
    const fixture = await startAppFixture({ runHost });
    try {
      expect(await probe(fixture.httpUrl, "/health")).toEqual({ status: 200, body: { ok: true } });
      expect(await probe(fixture.httpUrl, "/ready")).toEqual({ status: 200, body: { ready: true } });

      const inFlight = acceptInput(fixture.client, validCommand("rin_probe"), authMetadata());
      await waitFor(() => runHost.sessionIds.length === 1);
      const shutdown = fixture.app.shutdown();
      expect(fixture.app.lifecycle.ready()).toEqual({ ready: false });
      await expectProbeStoppedOrNotReady(fixture.httpUrl, "/ready");
      acceptGate.resolve();
      await expect(inFlight).resolves.toMatchObject({
        status: RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_ACCEPTED,
        sessionId: "sesn_1",
        runtimeInputId: "rin_probe",
      });
      await shutdown;
    } finally {
      await fixture.stop();
    }
  });

  test("wraps real gRPC command handlers in lifecycle shutdown drain", async () => {
    const acceptGate = deferred<void>();
    const runHost = new RecordingRunHost(acceptGate.promise);
    const fixture = await startAppFixture({ runHost });
    try {
      const inFlight = acceptInput(fixture.client, validCommand("rin_inflight"), authMetadata());
      await waitFor(() => runHost.sessionIds.length === 1);
      const shutdown = fixture.app.shutdown();

      await expectGrpcCode(fixture.app.service.acceptInput(validCommand("rin_direct_new"), authMetadata()), status.FAILED_PRECONDITION);
      const newGrpcCall = acceptInput(fixture.client, validCommand("rin_new_grpc"), authMetadata());
      void newGrpcCall.catch(() => undefined);
      acceptGate.resolve();

      await expect(inFlight).resolves.toMatchObject({
        status: RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_ACCEPTED,
        sessionId: "sesn_1",
        runtimeInputId: "rin_inflight",
        bindingId: "bind_1",
        bindingGeneration: 42,
      });
      await expectGrpcCodeOneOf(newGrpcCall, [
        status.FAILED_PRECONDITION,
        status.UNAVAILABLE,
      ]);
      await shutdown;
    } finally {
      await fixture.stop();
    }
  });

  test("serves real gRPC CleanupSession command handlers without owning durable binding release", async () => {
    const cleanupGate = deferred<{ readonly ok: true; readonly sessionId: string; readonly cleaned: boolean }>();
    const cleanupHost = new RecordingCleanupHost(cleanupGate.promise);
    const fixture = await startAppFixture({ cleanupRunHost: cleanupHost });
    try {
      const responsePromise = cleanup(fixture.client, validCleanupCommand("rin_cleanup"), authMetadata());
      await waitFor(() => cleanupHost.sessionIds.length === 1);
      let settled = false;
      void responsePromise.then(() => {
        settled = true;
      });
      await Promise.resolve();
      expect(settled).toBe(false);

      cleanupGate.resolve({ ok: true, sessionId: "sesn_1", cleaned: true });
      const response = await responsePromise;
      expect(response).toMatchObject({
        status: RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_ACCEPTED,
        sessionId: "sesn_1",
        runtimeInputId: "rin_cleanup",
        bindingId: "bind_1",
        bindingGeneration: 42,
      });
      expect(cleanupHost.scopes).toEqual([expect.objectContaining({ sessionId: "sesn_1", sessionThreadId: "thrd_1" })]);
      await waitFor(() => cleanupHost.completed);
    } finally {
      await fixture.stop();
    }
  });
});

async function startAppFixture(options: {
  readonly runHost?: RuntimeSessionRunHost;
  readonly cleanupRunHost?: RuntimeCoreCleanupHost;
  readonly tokenReviewClient?: RuntimeTokenReviewClient;
} = {}) {
  const app = createRuntimePodApp({
    config: validConfig(),
    logger: { info: () => undefined, error: () => undefined },
    tokenReviewClient: options.tokenReviewClient ?? new AllowingTokenReviewClient(),
    commandRunHost: options.runHost ?? new RecordingRunHost(Promise.resolve()),
    cleanupRunHost: options.cleanupRunHost ?? new RecordingCleanupHost(Promise.resolve({ ok: true, sessionId: "sesn_1", cleaned: true })),
    drainTimeoutMs: 250,
  });
  const started = await app.start();
  const grpcAddress = `127.0.0.1:${started.grpcPort}`;
  const client = new AgentRuntimePodServiceClient(grpcAddress, credentials.createInsecure());
  await waitForReady(client);
  return {
    app,
    client,
    httpUrl: started.httpUrl,
    stop: async () => {
      client.close();
      await app.shutdown().catch(() => undefined);
    },
  };
}

function validConfig(): RuntimePodConfig {
  return {
    ownPod: {
      namespace: "engine",
      name: "runtime-pod-a",
      uid: "uid-a",
      ip: "10.0.0.1",
    },
    deploymentEnvironment: "test",
    serviceVersion: "test",
    bridge: {
      namespace: "engine",
      serviceAccount: "bridge",
    },
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
  };
}

function validCommand(runtimeInputId: string): RuntimeInputCommandRequest {
  return {
    requestId: `req_${runtimeInputId}`,
    workspaceId: "wksp_1",
    sessionId: "sesn_1",
    sessionThreadId: "thrd_1",
    bindingId: "bind_1",
    bindingGeneration: 42,
    targetPodNamespace: "engine",
    targetPodName: "runtime-pod-a",
    targetPodUid: "uid-a",
    targetPodIp: "10.0.0.1",
    runtimeInputId,
    eventIds: ["sevt_1"],
    sequenceFrom: 1,
    sequenceTo: 1,
    commandKind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_MESSAGES,
    payloadJson: "",
  };
}

function validCleanupCommand(runtimeInputId: string): RuntimeInputCommandRequest {
  return {
    ...validCommand(runtimeInputId),
    eventIds: [],
    commandKind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_CLEANUP_SESSION,
    payloadJson: JSON.stringify({ reason: "expired" }),
  };
}

function authMetadata(): Metadata {
  const metadata = new Metadata();
  metadata.set("authorization", "bearer caller-token");
  return metadata;
}

async function acceptInput(client: AgentRuntimePodServiceClient, request: RuntimeInputCommandRequest, metadata: Metadata) {
  return await unary((callback) => {
    client.acceptInput(request, metadata, { deadline: Date.now() + 2_000 }, callback);
  });
}

async function cleanup(client: AgentRuntimePodServiceClient, request: RuntimeInputCommandRequest, metadata: Metadata) {
  return await unary((callback) => {
    client.cleanupSession(request, metadata, { deadline: Date.now() + 2_000 }, callback);
  });
}

async function unary(invoke: (callback: (error: Error | null, response: RuntimeInputCommandResponse) => void) => void) {
  return await new Promise<RuntimeInputCommandResponse>((resolve, reject) => {
    invoke((error, response) => {
      if (error !== null) {
        reject(error);
        return;
      }
      resolve(response);
    });
  });
}

async function expectGrpcCode(promise: Promise<unknown>, code: status): Promise<void> {
  await expectGrpcCodeOneOf(promise, [code]);
}

async function expectGrpcCodeOneOf(promise: Promise<unknown>, codes: readonly status[]): Promise<void> {
  try {
    await promise;
    throw new Error(`expected gRPC code ${codes.join(" or ")}`);
  } catch (error) {
    const code = (error as { readonly code?: status }).code;
    if (code === undefined) {
      throw error;
    }
    expect(codes).toContain(code);
  }
}

async function probe(baseUrl: URL, path: string): Promise<{ readonly status: number; readonly body: unknown }> {
  const response = await fetch(new URL(path, baseUrl));
  return { status: response.status, body: await response.json() };
}

async function expectProbeStoppedOrNotReady(baseUrl: URL, path: string): Promise<void> {
  let response: Awaited<ReturnType<typeof probe>>;
  try {
    response = await probe(baseUrl, path);
  } catch (error) {
    expect(error).toBeInstanceOf(Error);
    return;
  }
  expect(response).toEqual({ status: 503, body: { ready: false } });
}

async function waitFor(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 20; attempt += 1) {
    if (predicate()) {
      return;
    }
    await new Promise<void>((resolve) => {
      setTimeout(resolve, 5);
    });
  }
  throw new Error("condition was not met");
}

async function waitForReady(client: AgentRuntimePodServiceClient): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const deadline = new Date(Date.now() + 1_000);
    client.waitForReady(deadline, (error) => {
      if (error != null) {
        reject(error);
        return;
      }
      resolve();
    });
  });
}

function deferred<T>() {
  let resolve: (value: T) => void = () => undefined;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

class AllowingTokenReviewClient implements RuntimeTokenReviewClient {
  async createTokenReview() {
    return {
      authenticated: true,
      audiences: ["tetral-internal-grpc"],
      username: "system:serviceaccount:engine:bridge",
    };
  }
}

class RecordingRunHost implements RuntimeSessionRunHost {
  readonly sessionIds: string[] = [];

  constructor(private readonly gate: Promise<void>) {}

  async handleAcceptInput(command: Parameters<RuntimeSessionRunHost["handleAcceptInput"]>[0]) {
    this.sessionIds.push(command.sessionId);
    await this.gate;
    return { ok: true as const, sessionId: command.sessionId, created: true, started: true };
  }

  async handleAgentMail(command: Parameters<RuntimeSessionRunHost["handleAgentMail"]>[0]) {
    return { ok: true as const, sessionId: command.sessionId, applied: true };
  }

  async handleInterruptControl(sessionId: string) {
    return { ok: true as const, sessionId, created: false, interrupted: true, idleInterrupt: false };
  }

  async handleToolConfirmation(sessionId: string) {
    return { ok: true as const, sessionId, created: false, applied: true };
  }

  async handleTaskNotification(sessionId: string) {
    return { ok: true as const, sessionId, created: false, applied: true };
  }

  async handleRuntimeConfigPatch(sessionId: string) {
    return { ok: true as const, sessionId, created: false, applied: true };
  }
}

class RecordingCleanupHost implements RuntimeCoreCleanupHost {
  readonly sessionIds: string[] = [];
  readonly scopes: Array<Parameters<RuntimeCoreCleanupHost["handleCleanupSession"]>[0]> = [];
  completed = false;

  constructor(private readonly result: Promise<{ readonly ok: true; readonly sessionId: string; readonly cleaned: boolean }>) {}

  async handleCleanupSession(scope: Parameters<RuntimeCoreCleanupHost["handleCleanupSession"]>[0]) {
    this.sessionIds.push(scope.sessionId);
    this.scopes.push(scope);
    const result = await this.result;
    this.completed = true;
    return result;
  }
}
