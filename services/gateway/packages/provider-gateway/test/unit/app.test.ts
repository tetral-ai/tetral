import { describe, expect, test } from "bun:test";
import { createHmac } from "node:crypto";
import { credentials, Metadata, status } from "@grpc/grpc-js";
import {
  ProviderFinishReason,
  ProviderGatewayServiceClient,
  ProviderStreamEventType,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { createProviderGatewayApp } from "../../src/app.js";
import { GrpcStatusError } from "../../src/errors.js";
import { validProviderRequest } from "./fixtures.js";
import type { GatewayTokenReviewClient } from "../../src/auth.js";
import type { ProviderGatewayConfig } from "../../src/config.js";
import type { ProviderStreamEvent } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";

const RuntimePodUid = "pod_uid_gateway_app";
const BindingTokenKey = "gateway-runtime-binding-token-test-key-32";

describe("ProviderGatewayApp lifecycle", () => {
  test("graceful shutdown flips readiness before draining in-flight gRPC streams", async () => {
    let releaseStream = (): void => undefined;
    let shutdownPromise: Promise<void> | undefined;
    const request = validAnthropicProviderRequest();
    const app = createProviderGatewayApp({
      config: validConfig(),
      logger: { info: () => undefined, error: () => undefined },
      tokenReviewClient: new AllowingTokenReviewClient(),
      providerStreamer: {
        stream: async function* () {
          yield textEvent(request, ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START, "");
          await new Promise<void>((resolve) => {
            releaseStream = resolve;
          });
          yield finishEvent(request);
        },
      },
    });
    const started = await app.start();
    const client = new ProviderGatewayServiceClient(`127.0.0.1:${started.grpcPort}`, credentials.createInsecure());
    try {
      expect(await jsonProbe(started.httpUrl, "/healthz")).toEqual({ status: 200, body: { ok: true } });
      expect(await jsonProbe(started.httpUrl, "/readyz")).toEqual({ status: 200, body: { ready: true } });
      const metrics = await textProbe(started.httpUrl, "/metrics");
      expect(metrics.status).toBe(200);
      expect(metrics.body).toContain("providergateway_ready 1");
      expect(metrics.body).toContain("providergateway_provider_streams_active");

      const call = client.streamProviderRequest(request, metadata());
      const eventsPromise = collectEvents(call);
      await onceData(call);

      shutdownPromise = app.shutdown();
      await waitUntil(() => !app.ready().ready);
      await expectGrpcCode(collectEvents(app.service.streamProviderRequest(request, metadata())), status.UNAVAILABLE);

      releaseStream();
      const events = await eventsPromise;
      await shutdownPromise;

      expect(events.map((event) => event.type)).toEqual([
        ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START,
        ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
      ]);
    } finally {
      releaseStream();
      await shutdownPromise?.catch(() => undefined);
      client.close();
      await app.shutdown();
    }
  });
});

function validConfig(): ProviderGatewayConfig {
  return {
    deploymentEnvironment: "test",
    serviceVersion: "test",
    grpcBindAddress: "127.0.0.1:0",
    httpBindAddress: "127.0.0.1:0",
    allowedRuntimePod: {
      namespace: "tetral-agent-runtime",
      serviceAccount: "agent-runtime",
    },
    runtimeBindingTokenHMACKey: BindingTokenKey,
    databaseUrl: "postgres://gateway-readonly.example/tetral",
    databasePool: {
      max: 10,
      idleTimeout: 30,
      maxLifetime: 1_800,
      connectionTimeout: 30,
      statementTimeoutMs: 30_000,
    },
    vaultKeyHex: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    kubernetesApiServerUrl: "https://kubernetes.default.svc",
    kubernetesApiCaCertPath: "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
    tokenReviewReviewerTokenPath: "/var/run/secrets/kubernetes.io/serviceaccount/token",
    bridgeApiGrpcAddress: "bridge.tetral-system.svc.cluster.local:9090",
    bridgeTokenPath: "/var/run/secrets/tetral-internal-grpc/bridge/token",
    maxConcurrentTurns: 8,
  };
}

function validAnthropicProviderRequest() {
  const request = validProviderRequest({
    model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "" },
    attachments: [],
  });
  return {
    ...request,
    runtimeBindingToken: signedRuntimeBindingToken(request, RuntimePodUid),
  };
}

function metadata(): Metadata {
  const value = new Metadata();
  value.set("authorization", "bearer request-token");
  return value;
}

function textEvent(
  request: { readonly requestId: string; readonly modelRequestId: string },
  type: ProviderStreamEventType,
  text: string,
): ProviderStreamEvent {
  return {
    requestId: request.requestId,
    modelRequestId: request.modelRequestId,
    type,
    text: {
      id: "text_1",
      text,
      metadataJson: "{}",
    },
  };
}

function finishEvent(request: { readonly requestId: string; readonly modelRequestId: string }): ProviderStreamEvent {
  return {
    requestId: request.requestId,
    modelRequestId: request.modelRequestId,
    type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
    finish: {
      reason: ProviderFinishReason.PROVIDER_FINISH_REASON_STOP,
      usage: {
        inputTotalTokens: 1,
        inputUncachedTokens: 1,
        outputTotalTokens: 1,
        totalTokens: 2,
        providerUsageJson: "{}",
      },
      metadataJson: "{}",
    },
  };
}

async function collectEvents(events: AsyncIterable<ProviderStreamEvent>): Promise<readonly ProviderStreamEvent[]> {
  const out: ProviderStreamEvent[] = [];
  for await (const event of events) {
    out.push(event);
  }
  return out;
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

async function jsonProbe(base: URL, path: string): Promise<{ readonly status: number; readonly body: unknown }> {
  const response = await fetch(new URL(path, base));
  return { status: response.status, body: await response.json() };
}

async function textProbe(base: URL, path: string): Promise<{ readonly status: number; readonly body: string }> {
  const response = await fetch(new URL(path, base));
  return { status: response.status, body: await response.text() };
}

async function onceData(call: NodeJS.EventEmitter): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const cleanup = (): void => {
      call.off("data", onData);
      call.off("error", onError);
    };
    const onData = (): void => {
      cleanup();
      resolve();
    };
    const onError = (error: unknown): void => {
      cleanup();
      reject(error);
    };
    call.once("data", onData);
    call.once("error", onError);
  });
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

function signedRuntimeBindingToken(request: {
  readonly workspaceId: string;
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly bindingId: string;
  readonly bindingGeneration: number;
}, runtimePodUid: string, expiresAt = "2027-01-01T00:05:00Z"): string {
  const payload = JSON.stringify({
    v: 1,
    workspace_id: request.workspaceId,
    session_id: request.sessionId,
    session_thread_id: request.sessionThreadId,
    binding_id: request.bindingId,
    binding_generation: request.bindingGeneration,
    runtime_pod_uid: runtimePodUid,
    exp: Math.floor(new Date(expiresAt).getTime() / 1000),
  });
  const payloadPart = Buffer.from(payload, "utf8").toString("base64url");
  const signaturePart = createHmac("sha256", BindingTokenKey).update(payloadPart).digest("base64url");
  return `rtbt_v1.${payloadPart}.${signaturePart}`;
}

class AllowingTokenReviewClient implements GatewayTokenReviewClient {
  async createTokenReview() {
    return {
      authenticated: true,
      audiences: ["tetral-internal-grpc"],
      username: "system:serviceaccount:tetral-agent-runtime:agent-runtime",
      podUid: RuntimePodUid,
    };
  }
}
