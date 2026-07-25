import { describe, expect, test } from "bun:test";
import { Metadata } from "@grpc/grpc-js";
import { authenticateMcpCaller } from "../../src/auth.js";
import type { McpTokenReviewClient } from "../../src/auth.js";

const RunMcpToolMethod = "/tetral.provider_gateway.v1.McpConnectorService/RunMcpTool";
const ListMcpToolsMethod = "/tetral.provider_gateway.v1.McpConnectorService/ListMcpTools";
const Audience = "tetral-internal-grpc";
const RuntimeUsername = "system:serviceaccount:tetral-agent-runtime:agent-runtime";
const BridgeUsername = "system:serviceaccount:tetral-system:bridge";
const RuntimePodUid = "pod_uid_mcp_connector";
const AllowedRuntimePod = { namespace: "tetral-agent-runtime", name: "agent-runtime" };
const AllowedBridge = { namespace: "tetral-system", name: "bridge" };

describe("MCP connector TokenReview caller authentication", () => {
  test("accepts the configured Runtime Pod only for RunMcpTool", async () => {
    const tokenReviewClient = new RecordingTokenReviewClient();

    const result = await authenticateMcpCaller({
      metadata: metadata("Bearer projected-token"),
      method: RunMcpToolMethod,
      tokenReviewClient,
      allowedRuntimePod: AllowedRuntimePod,
      allowedBridge: AllowedBridge,
    });

    expect(result).toEqual({ ok: true, serviceAccount: { ...AllowedRuntimePod, podUid: RuntimePodUid } });
    expect(tokenReviewClient.calls).toEqual([{ token: "projected-token", audiences: [Audience] }]);
  });

  test("accepts the configured Bridge only for ListMcpTools", async () => {
    const result = await authenticateMcpCaller({
      metadata: metadata("Bearer projected-token"),
      method: ListMcpToolsMethod,
      tokenReviewClient: new RecordingTokenReviewClient({
        authenticated: true,
        audiences: [Audience],
        username: BridgeUsername,
        podUid: RuntimePodUid,
      }),
      allowedRuntimePod: AllowedRuntimePod,
      allowedBridge: AllowedBridge,
    });

    expect(result).toEqual({ ok: true, serviceAccount: { ...AllowedBridge, podUid: RuntimePodUid } });
  });

  test("rejects malformed bearer metadata before authorization", async () => {
    for (const value of [undefined, "Basic abc", "Bearer", "Bearer "]) {
      const result = await authenticateMcpCaller({
        metadata: metadata(value),
        method: RunMcpToolMethod,
        tokenReviewClient: new RecordingTokenReviewClient(),
        allowedRuntimePod: AllowedRuntimePod,
        allowedBridge: AllowedBridge,
      });
      expect(result).toEqual({ ok: false, code: "Unauthenticated", message: "unauthenticated" });
    }
  });

  test("rejects unrecognized methods and callers on the wrong connector method", async () => {
    for (const input of [
      { method: "/tetral.provider_gateway.v1.ProviderGatewayService/RunWeb", username: RuntimeUsername },
      { method: RunMcpToolMethod, username: BridgeUsername },
      { method: ListMcpToolsMethod, username: RuntimeUsername },
      { method: ListMcpToolsMethod, username: "system:serviceaccount:tetral-system:api" },
    ]) {
      const result = await authenticateMcpCaller({
        metadata: metadata("Bearer projected-token"),
        method: input.method,
        tokenReviewClient: new RecordingTokenReviewClient({
          authenticated: true,
          audiences: [Audience],
          username: input.username,
          podUid: RuntimePodUid,
        }),
        allowedRuntimePod: AllowedRuntimePod,
        allowedBridge: AllowedBridge,
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

class RecordingTokenReviewClient implements McpTokenReviewClient {
  readonly calls: Array<{ readonly token: string; readonly audiences: readonly string[] }> = [];

  constructor(
    private readonly result: Awaited<ReturnType<McpTokenReviewClient["createTokenReview"]>> = {
      authenticated: true,
      audiences: [Audience],
      username: RuntimeUsername,
      podUid: RuntimePodUid,
    },
  ) {}

  async createTokenReview(input: Parameters<McpTokenReviewClient["createTokenReview"]>[0]) {
    this.calls.push(input);
    return this.result;
  }
}
