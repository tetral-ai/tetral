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
const AllowedJobRunner = { namespace: "tetral-system", name: "job-runner" };
const AllowedBridge = { namespace: "tetral-system", name: "bridge" };

describe("MCP connector TokenReview caller authentication", () => {
  test("accepts the configured Runtime Pod only for RunMcpTool", async () => {
    const tokenReviewClient = new RecordingTokenReviewClient();

    const result = await authenticateMcpCaller({
      metadata: metadata("Bearer projected-token"),
      method: RunMcpToolMethod,
      tokenReviewClient,
      allowedRuntimePod: AllowedRuntimePod,
      allowedDiscoveryCallers: [AllowedBridge, AllowedJobRunner],
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
      allowedDiscoveryCallers: [AllowedBridge, AllowedJobRunner],
    });

    expect(result).toEqual({ ok: true, serviceAccount: { ...AllowedBridge, podUid: RuntimePodUid } });
  });

  test("Job Runner discovery does not grant tool execution",async()=>{
    for(const method of [ListMcpToolsMethod,RunMcpToolMethod]){
      const result=await authenticateMcpCaller({metadata:metadata("Bearer projected-token"),method,tokenReviewClient:new RecordingTokenReviewClient({authenticated:true,audiences:[Audience],username:"system:serviceaccount:tetral-system:job-runner",podUid:RuntimePodUid}),allowedRuntimePod:AllowedRuntimePod,allowedDiscoveryCallers:[AllowedBridge,AllowedJobRunner]});
      expect(result).toEqual(method===ListMcpToolsMethod?{ok:true,serviceAccount:{...AllowedJobRunner,podUid:RuntimePodUid}}:{ok:false,code:"PermissionDenied",message:"permission denied"});
    }
  });
  test("TokenReview expiration, audience and pod binding fail closed",async()=>{
    for(const review of [
      {authenticated:false,audiences:[Audience],username:RuntimeUsername,podUid:RuntimePodUid},
      {authenticated:true,audiences:["api"],username:RuntimeUsername,podUid:RuntimePodUid},
      {authenticated:true,audiences:[Audience],username:RuntimeUsername,podUid:""},
    ]){
      expect(await authenticateMcpCaller({metadata:metadata("Bearer projected-token"),method:RunMcpToolMethod,tokenReviewClient:new RecordingTokenReviewClient(review),allowedRuntimePod:AllowedRuntimePod,allowedDiscoveryCallers:[AllowedBridge,AllowedJobRunner]})).toEqual({ok:false,code:"Unauthenticated",message:"unauthenticated"});
    }
  });
  test("rejects malformed bearer metadata before authorization", async () => {
    for (const value of [undefined, "Basic abc", "Bearer", "Bearer "]) {
      const result = await authenticateMcpCaller({
        metadata: metadata(value),
        method: RunMcpToolMethod,
        tokenReviewClient: new RecordingTokenReviewClient(),
        allowedRuntimePod: AllowedRuntimePod,
        allowedDiscoveryCallers: [AllowedBridge, AllowedJobRunner],
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
 {method:ListMcpToolsMethod,username:"system:serviceaccount:wrong-system:job-runner"},
 {method:RunMcpToolMethod,username:"system:serviceaccount:tetral-system:provider-gateway"},
 {method:ListMcpToolsMethod,username:"system:serviceaccount:tetral-system:gateway"},
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
        allowedDiscoveryCallers: [AllowedBridge, AllowedJobRunner],
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
