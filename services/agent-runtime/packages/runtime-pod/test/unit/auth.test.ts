import { describe, expect, test } from "bun:test";
import { mkdtemp, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { Metadata } from "@grpc/grpc-js";
import {
  authenticateRuntimeCaller,
  buildOutboundBearerMetadata,
  KubernetesTokenReviewClient,
} from "../../src/auth.js";
import type { BunTokenReviewRequestInit, RuntimeTokenReviewClient } from "../../src/auth.js";

describe("Runtime Pod gRPC auth", () => {
  test("outbound auth attaches one bearer value and redacts token errors", async () => {
    const dir = await mkdtemp(join(tmpdir(), "runtime-auth-"));
    const tokenPath = join(dir, "token");
    await writeFile(tokenPath, "secret-token\n", { mode: 0o600 });
    const metadata = await buildOutboundBearerMetadata({ tokenPath });
    expect(metadata.get("authorization")).toEqual(["bearer secret-token"]);

    await expect(buildOutboundBearerMetadata({ tokenPath: join(dir, "missing") })).rejects.toThrow("service account token unavailable");

    const malformedTokenPath = join(dir, "malformed-token");
    await writeFile(malformedTokenPath, "secret-token with-space", { mode: 0o600 });
    try {
      await buildOutboundBearerMetadata({ tokenPath: malformedTokenPath });
      throw new Error("malformed token was accepted");
    } catch (err) {
      const message = String((err as Error).message);
      expect(message).toBe("service account token unavailable");
      expect(message).not.toContain("secret-token");
      expect(message).not.toContain("bearer secret-token");
    }
  });

  test("inbound TokenReview admits the closed seven-method Bridge command set and denies an eighth", async () => {
    const allowedMethods = [
      "/tetral.agent_runtime.v1.AgentRuntimePodService/AcceptInput",
      "/tetral.agent_runtime.v1.AgentRuntimePodService/AcceptAgentMail",
      "/tetral.agent_runtime.v1.AgentRuntimePodService/AcceptTaskNotification",
      "/tetral.agent_runtime.v1.AgentRuntimePodService/Interrupt",
      "/tetral.agent_runtime.v1.AgentRuntimePodService/ResolveToolConfirmation",
      "/tetral.agent_runtime.v1.AgentRuntimePodService/ApplyRuntimeConfig",
      "/tetral.agent_runtime.v1.AgentRuntimePodService/CleanupSession",
    ] as const;
    expect(allowedMethods).toHaveLength(7);
    for (const method of allowedMethods) {
      const client = new RecordingTokenReviewClient(validReview());
      const metadata = metadataWithAuthorization("bearer caller-token");
      const result = await authenticateRuntimeCaller({
        metadata,
        method,
        tokenReviewClient: client,
        allowedBridge: allowedBridge(),
      });
      expect(result.ok, method).toBe(true);
      expect(client.token).toBe("caller-token");
      expect(client.audiences).toEqual(["tetral-internal-grpc"]);
    }

    const denied = await authenticateRuntimeCaller({
      metadata: metadataWithAuthorization("bearer caller-token"),
      method: "/tetral.agent_runtime.v1.AgentRuntimePodService/UnlistedEighthCommand",
      tokenReviewClient: new RecordingTokenReviewClient(validReview()),
      allowedBridge: allowedBridge(),
    });
    expect(denied).toEqual({ ok: false, code: "PermissionDenied", message: "permission denied" });
  });

  test("inbound TokenReview rejects unsafe callers with canonical codes", async () => {
    for (const scenario of [
      { name: "missing bearer", metadata: new Metadata(), review: validReview(), code: "Unauthenticated" as const },
      { name: "malformed bearer", metadata: metadataWithAuthorization("bearer one two"), review: validReview(), code: "Unauthenticated" as const },
      { name: "duplicate bearer", metadata: metadataWithAuthorizationValues(["bearer one", "bearer two"]), review: validReview(), code: "Unauthenticated" as const },
      {
        name: "token review error",
        metadata: metadataWithAuthorization("bearer token"),
        review: validReview(),
        reviewError: new Error(`secret token body https://kubernetes.default.svc {"kind":"TokenReview","status":{"error":"kube object dump"}} raw request body sk-provider-key`),
        code: "Unauthenticated" as const,
      },
      { name: "wrong audience", metadata: metadataWithAuthorization("bearer token"), review: { ...validReview(), audiences: ["other"] }, code: "Unauthenticated" as const },
      { name: "unauthenticated", metadata: metadataWithAuthorization("bearer token"), review: { ...validReview(), authenticated: false }, code: "Unauthenticated" as const },
      { name: "malformed username", metadata: metadataWithAuthorization("bearer token"), review: { ...validReview(), username: "engine/bridge" }, code: "Unauthenticated" as const },
      { name: "not allowlisted", metadata: metadataWithAuthorization("bearer token"), review: { ...validReview(), username: "system:serviceaccount:engine:api" }, code: "PermissionDenied" as const },
      { name: "caller metadata spoof ignored", metadata: metadataWithAuthorization("bearer token", "system:serviceaccount:engine:bridge"), review: { ...validReview(), username: "system:serviceaccount:engine:api" }, code: "PermissionDenied" as const },
    ]) {
      const result = await authenticateRuntimeCaller({
        metadata: scenario.metadata,
        method: "/tetral.agent_runtime.v1.AgentRuntimePodService/CleanupSession",
        tokenReviewClient: new RecordingTokenReviewClient(scenario.review, scenario.reviewError),
        allowedBridge: allowedBridge(),
      });
      expect(result.ok, scenario.name).toBe(false);
      if (!result.ok) {
        expect(result.code).toBe(scenario.code);
        for (const forbidden of ["token", "secret", "kubernetes.default.svc", "TokenReview", "kube object dump", "raw request body", "sk-provider-key"]) {
          expect(result.message).not.toContain(forbidden);
        }
      }
    }
  });

  test("inbound TokenReview rejects before request handling side effects", async () => {
    const sideEffects = new SideEffectRecorder();
    const response = await authenticateAndHandle({
      metadata: metadataWithAuthorization("bearer request-token", "system:serviceaccount:engine:bridge"),
      method: "/tetral.agent_runtime.v1.AgentRuntimePodService/AcceptInput",
      tokenReviewClient: new RecordingTokenReviewClient({ ...validReview(), username: "system:serviceaccount:engine:api" }),
      handler: () => {
        sideEffects.validateRequest();
        sideEffects.writeContext();
        sideEffects.callCore();
        sideEffects.cleanup();
        sideEffects.unbind();
        sideEffects.logRequestControlledId();
      },
    });
    expect(response).toEqual({ ok: false, code: "PermissionDenied", message: "permission denied" });
    expect(sideEffects.count).toBe(0);
  });

  test("KubernetesTokenReviewClient sends TokenReview with internal audience, reviewer bearer, and Kubernetes API CA trust", async () => {
    const dir = await mkdtemp(join(tmpdir(), "runtime-review-"));
    const reviewerTokenPath = join(dir, "reviewer-token");
    const apiServerCaCertPath = join(dir, "kubernetes-ca.crt");
    await writeFile(reviewerTokenPath, "reviewer-token\n", { mode: 0o600 });
    await writeFile(apiServerCaCertPath, "-----BEGIN CERTIFICATE-----\nca\n-----END CERTIFICATE-----\n", { mode: 0o600 });
    const requests: BunTokenReviewRequestInit[] = [];
    const client = new KubernetesTokenReviewClient({
      apiServerUrl: "https://kubernetes.default.svc/",
      reviewerTokenPath,
      apiServerCaCertPath,
      fetchImpl: async (_url, init) => {
        requests.push(init);
        return new Response(JSON.stringify({
        status: {
          authenticated: true,
          audiences: ["tetral-internal-grpc"],
          user: { username: "system:serviceaccount:engine:bridge" },
        },
      }), { status: 201 });
      },
    });
    const review = await client.createTokenReview({ token: "caller-token", audiences: ["tetral-internal-grpc"] });
    expect(review.authenticated).toBe(true);
    expect(requests).toHaveLength(1);
    expect((requests[0]?.headers as Record<string, string>).authorization).toBe("bearer reviewer-token");
    const ca = requests[0]?.tls.ca[0];
    expect(ca !== undefined && "name" in ca ? ca.name : undefined).toBe(apiServerCaCertPath);
    expect(String(requests[0]?.body)).toContain("\"audiences\":[\"tetral-internal-grpc\"]");
  });
});

function metadataWithAuthorization(value: string, spoofedIdentity?: string): Metadata {
  const metadata = new Metadata();
  metadata.set("authorization", value);
  if (spoofedIdentity !== undefined) {
    metadata.set("caller.service_account", spoofedIdentity);
  }
  return metadata;
}

function metadataWithAuthorizationValues(values: string[]): Metadata {
  const metadata = new Metadata();
  for (const value of values) {
    metadata.add("authorization", value);
  }
  return metadata;
}

function allowedBridge() {
  return { namespace: "engine", name: "bridge" };
}

function validReview() {
  return {
    authenticated: true,
    audiences: ["tetral-internal-grpc"],
    username: "system:serviceaccount:engine:bridge",
  };
}

async function authenticateAndHandle(input: {
  readonly metadata: Metadata;
  readonly method: string;
  readonly tokenReviewClient: RuntimeTokenReviewClient;
  readonly handler: () => void;
}) {
  const auth = await authenticateRuntimeCaller({
    metadata: input.metadata,
    method: input.method,
    tokenReviewClient: input.tokenReviewClient,
    allowedBridge: allowedBridge(),
  });
  if (!auth.ok) {
    return auth;
  }
  input.handler();
  return { ok: true as const };
}

class SideEffectRecorder {
  count = 0;

  validateRequest() { this.count++; }
  writeContext() { this.count++; }
  callCore() { this.count++; }
  cleanup() { this.count++; }
  unbind() { this.count++; }
  logRequestControlledId() { this.count++; }
}

class RecordingTokenReviewClient implements RuntimeTokenReviewClient {
  token = "";
  audiences: readonly string[] = [];

  constructor(
    private readonly response: { readonly authenticated: boolean; readonly audiences: readonly string[]; readonly username: string },
    private readonly err?: Error,
  ) {}

  async createTokenReview(input: { readonly token: string; readonly audiences: readonly string[] }) {
    this.token = input.token;
    this.audiences = input.audiences;
    if (this.err !== undefined) {
      throw this.err;
    }
    return this.response;
  }
}
