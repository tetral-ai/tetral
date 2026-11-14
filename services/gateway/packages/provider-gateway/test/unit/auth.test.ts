import { describe, expect, test } from "bun:test";
import { causeOf } from "@tetral/ts-observability";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Metadata } from "@grpc/grpc-js";
import { authenticateGatewayCaller, buildOutboundBearerMetadata, validateKubernetesTokenReviewReviewerMaterial } from "../../src/auth.js";
import type { GatewayTokenReviewClient } from "../../src/auth.js";

const Method = "/tetral.provider_gateway.v1.ProviderGatewayService/StreamProviderRequest";
const Audience = "tetral-internal-grpc";
const RuntimeUsername = "system:serviceaccount:tetral-agent-runtime:agent-runtime";
const RuntimePodUid = "pod_uid_gateway";
const AllowedRuntimePod = { namespace: "tetral-agent-runtime", name: "agent-runtime" };

describe("Gateway TokenReview caller authentication", () => {
  test("accepts only the configured Runtime Pod service account with the internal audience", async () => {
    const tokenReviewClient = new RecordingTokenReviewClient({
      authenticated: true,
      audiences: [Audience],
      username: RuntimeUsername,
      podUid: RuntimePodUid,
    });

    const result = await authenticateGatewayCaller({
      metadata: metadata("Bearer projected-token"),
      method: Method,
      tokenReviewClient,
      allowedRuntimePod: AllowedRuntimePod,
    });

    expect(result).toEqual({ ok: true, serviceAccount: { ...AllowedRuntimePod, podUid: RuntimePodUid } });
    expect(tokenReviewClient.calls).toEqual([{ token: "projected-token", audiences: [Audience] }]);
  });

  test("rejects missing or malformed bearer metadata before authorization", async () => {
    for (const value of [undefined, "Basic abc", "Bearer", "Bearer "]) {
      const result = await authenticateGatewayCaller({
        metadata: metadata(value),
        method: Method,
        tokenReviewClient: new RecordingTokenReviewClient(),
        allowedRuntimePod: AllowedRuntimePod,
      });
      expect(result).toEqual({ ok: false, code: "Unauthenticated", message: "unauthenticated" });
    }
  });

  test("rejects failed TokenReview, wrong audience, and non-service-account users", async () => {
    for (const tokenReviewClient of [
      new ThrowingTokenReviewClient(),
      new RecordingTokenReviewClient({ authenticated: false, audiences: [Audience], username: RuntimeUsername, podUid: RuntimePodUid }),
      new RecordingTokenReviewClient({ authenticated: true, audiences: [], username: RuntimeUsername, podUid: RuntimePodUid }),
      new RecordingTokenReviewClient({ authenticated: true, audiences: [Audience], username: "system:user:alice", podUid: RuntimePodUid }),
      new RecordingTokenReviewClient({ authenticated: true, audiences: [Audience], username: RuntimeUsername, podUid: "" }),
    ]) {
      const result = await authenticateGatewayCaller({
        metadata: metadata("Bearer projected-token"),
        method: Method,
        tokenReviewClient,
        allowedRuntimePod: AllowedRuntimePod,
      });
      expect(result).toMatchObject({ ok: false, code: "Unauthenticated", message: "unauthenticated" });
      // A review that could not be made gives its error to the record.
      expect((result as { cause?: unknown }).cause).toEqual(tokenReviewClient instanceof ThrowingTokenReviewClient ? new Error("token review unavailable") : undefined);
    }
  });

  test("rejects unrecognized methods and non-runtime service accounts", async () => {
    for (const input of [
      {
        method: "/tetral.provider_gateway.v1.ProviderGatewayService/Unknown",
        username: RuntimeUsername,
      },
      {
        method: Method,
        username: "system:serviceaccount:tetral-system:api",
      },
    ]) {
      const result = await authenticateGatewayCaller({
        metadata: metadata("Bearer projected-token"),
        method: input.method,
        tokenReviewClient: new RecordingTokenReviewClient({
          authenticated: true,
          audiences: [Audience],
          username: input.username,
          podUid: RuntimePodUid,
        }),
        allowedRuntimePod: AllowedRuntimePod,
      });
      expect(result).toEqual({ ok: false, code: "PermissionDenied", message: "permission denied" });
    }
  });
});

function metadata(authorization: string | undefined): Metadata {
  const value = new Metadata();
  if (authorization !== undefined) {
    value.set("authorization", authorization);
  }
  return value;
}

class RecordingTokenReviewClient implements GatewayTokenReviewClient {
  readonly calls: Array<{ readonly token: string; readonly audiences: readonly string[] }> = [];

  constructor(
    private readonly result: Awaited<ReturnType<GatewayTokenReviewClient["createTokenReview"]>> = {
      authenticated: true,
      audiences: [Audience],
      username: RuntimeUsername,
      podUid: RuntimePodUid,
    },
  ) {}

  async createTokenReview(input: Parameters<GatewayTokenReviewClient["createTokenReview"]>[0]) {
    this.calls.push(input);
    return this.result;
  }
}

class ThrowingTokenReviewClient implements GatewayTokenReviewClient {
  async createTokenReview(): Promise<Awaited<ReturnType<GatewayTokenReviewClient["createTokenReview"]>>> {
    throw new Error("token review unavailable");
  }
}

describe("auth file reads", () => {
  test("a file the auth cannot read is the cause of its error", async () => {
    const dir = await mkdtemp(join(tmpdir(), "auth-files-"));
    const present = join(dir, "present");
    await writeFile(present, "token\n", { mode: 0o600 });
    const missing = join(dir, "missing");
    // The mounted path stays out of the cause.
    for (const [read, cause] of [
      [() => buildOutboundBearerMetadata({ tokenPath: missing }), "service account token unavailable: ENOENT: no such file or directory, open"],
      [() => validateKubernetesTokenReviewReviewerMaterial({ reviewerTokenPath: missing, apiServerCaCertPath: present }), "service account token unavailable: ENOENT: no such file or directory, open"],
      [() => validateKubernetesTokenReviewReviewerMaterial({ reviewerTokenPath: present, apiServerCaCertPath: missing }), "kubernetes api ca material unavailable: ENOENT: no such file or directory, open"],
    ] as const) {
      const failure = await read().then(() => undefined, (error: unknown) => error);
      expect(causeOf(failure).text).toBe(cause);
    }
  });
});
