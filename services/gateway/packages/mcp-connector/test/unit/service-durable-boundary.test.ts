import { describe, expect, test } from "bun:test";
import { createHmac } from "node:crypto";
import { Metadata } from "@grpc/grpc-js";
import { createRuntimeBindingTokenVerifier } from "@tetral/gateway-protocol/src/binding-token.js";
import { McpErrorKind, RunMcpToolStatus } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { RunMcpToolRequest, RunMcpToolResponse } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { McpConnectorServiceShell } from "../../src/service.js";
import type { McpAuthenticator, McpClient, McpConnectorLogger } from "../../src/service.js";
import { InMemoryMcpIdempotencyStore } from "../../src/idempotency.js";
import type { IdempotencyClaim, IdempotencyKey, McpExecutorPayload, McpIdempotencyContext, McpIdempotencyStore, PendingStoredRunMcpToolResponse } from "../../src/idempotency.js";

const RuntimePodUid = "runtime-pod-uid";
const BindingTokenKey = "0123456789abcdef0123456789abcdef";
const executor: McpExecutorPayload = {
  mcpServerName: "github",
  toolName: "create_issue",
  inputJson: '{"title":"Bridge authority"}',
};

describe("MCP connector durable executor boundary", () => {
  test("executes only the Bridge-derived server, tool, and canonical input", async () => {
    const client = new RecordingMcpClient();
    const store = new RecordingStore({ status: "new", executor });
    const response = await createService(client, store).runMcpTool(validRunRequest(), authorizationMetadata());
    expect(response).toMatchObject({ status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED, resultText: "created" });
    expect(client.calls).toEqual([{
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      sessionThreadId: "thrd_1",
      mcpServerName: "github",
      toolName: "create_issue",
      input: { title: "Bridge authority" },
    }]);
    expect(store.claimContexts[0]?.claimId).toBe("mcpclaim_test");
    expect(store.storeContexts[0]?.claimId).toBe("mcpclaim_test");
  });

  test("replays an already completed durable result without calling the provider", async () => {
    const client = new RecordingMcpClient();
    const storedResponse: RunMcpToolResponse = {
      status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED,
      resultText: "replayed",
      attachments: [],
    };
    const store = new RecordingStore({
      status: "replay",
      stored: { response: storedResponse, contentItems: 1, refreshTriggered: false },
    });
    await expect(createService(client, store).runMcpTool(validRunRequest(), authorizationMetadata())).resolves.toMatchObject({ resultText: "replayed" });
    expect(client.calls).toEqual([]);
  });

  test("maps active remote claims and stale custody without provider execution", async () => {
    const inFlightClient = new RecordingMcpClient();
    const inFlight = await createService(inFlightClient, new RecordingStore({ status: "in_flight" })).runMcpTool(validRunRequest(), authorizationMetadata());
    expect(inFlight).toMatchObject({ status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_RUNTIME_ERROR, errorKind: McpErrorKind.MCP_ERROR_KIND_IN_FLIGHT });
    expect(inFlightClient.calls).toEqual([]);

    const staleClient = new RecordingMcpClient();
    const stale = await createService(staleClient, new RecordingStore({ status: "stale_custody" })).runMcpTool(validRunRequest(), authorizationMetadata());
    expect(stale).toMatchObject({ status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_RUNTIME_ERROR, errorKind: McpErrorKind.MCP_ERROR_KIND_CUSTODY_LOST });
    expect(staleClient.calls).toEqual([]);
  });

  test("rejects malformed Bridge executor payload before provider execution", async () => {
    const client = new RecordingMcpClient();
    const store = new RecordingStore({ status: "new", executor: { ...executor, inputJson: "[]" } });
    await expect(createService(client, store).runMcpTool(validRunRequest(), authorizationMetadata())).rejects.toMatchObject({ code: 3 });
    expect(client.calls).toEqual([]);
  });

  test("rejects a mismatched Runtime binding token before claiming", async () => {
    const client = new RecordingMcpClient();
    const store = new RecordingStore({ status: "new", executor });
    await expect(createService(client, store).runMcpTool({ ...validRunRequest(), bindingGeneration: 43 }, authorizationMetadata())).rejects.toMatchObject({ code: 7 });
    expect(store.claimContexts).toEqual([]);
  });
});

function createService(client: RecordingMcpClient, idempotencyStore: McpIdempotencyStore = new InMemoryMcpIdempotencyStore(executor)): McpConnectorServiceShell {
  const logger: McpConnectorLogger = { info: () => undefined, error: () => undefined };
  const authenticator: McpAuthenticator = {
    authenticate: async () => ({ ok: true, serviceAccount: { namespace: "tetral", name: "runtime", podUid: RuntimePodUid } }),
  };
  return new McpConnectorServiceShell({
    authenticator,
    runtimeBindingTokenVerifier: createRuntimeBindingTokenVerifier({ hmacKey: BindingTokenKey, now: () => new Date("2026-01-01T00:00:00Z") }),
    logger,
    ready: () => true,
    client,
    idempotencyStore,
    claimIdFactory: () => "mcpclaim_test",
  });
}

function validRunRequest(): RunMcpToolRequest {
  return {
    workspaceId: "wksp_1",
    sessionId: "sesn_1",
    sessionThreadId: "thrd_1",
    toolUseEventId: "sevt_tool_1",
    bindingId: "bind_1",
    bindingGeneration: 42,
    runtimeBindingToken: signedRuntimeBindingToken(),
  };
}

function signedRuntimeBindingToken(): string {
  const payloadPart = Buffer.from(JSON.stringify({
    v: 1,
    workspace_id: "wksp_1",
    session_id: "sesn_1",
    session_thread_id: "thrd_1",
    binding_id: "bind_1",
    binding_generation: 42,
    runtime_pod_uid: RuntimePodUid,
    exp: Math.floor(new Date("2026-01-01T00:05:00Z").getTime() / 1000),
  })).toString("base64url");
  return `rtbt_v1.${payloadPart}.${createHmac("sha256", BindingTokenKey).update(payloadPart).digest("base64url")}`;
}

function authorizationMetadata(): Metadata {
  const metadata = new Metadata();
  metadata.set("authorization", "Bearer internal");
  return metadata;
}

class RecordingMcpClient implements McpClient {
  readonly calls: Array<Parameters<McpClient["callTool"]>[0]> = [];
  async listTools() { return []; }
  async callTool(input: Parameters<McpClient["callTool"]>[0]) {
    this.calls.push(input);
    return { content: [{ type: "text" as const, text: "created" }] };
  }
}

class RecordingStore implements McpIdempotencyStore {
  readonly claimContexts: McpIdempotencyContext[] = [];
  readonly storeContexts: McpIdempotencyContext[] = [];
  constructor(private readonly result: IdempotencyClaim) {}
  claim(_key: IdempotencyKey, context?: McpIdempotencyContext): IdempotencyClaim {
    if (context !== undefined) this.claimContexts.push(context);
    return this.result;
  }
  store(_key: IdempotencyKey, stored: PendingStoredRunMcpToolResponse, context?: McpIdempotencyContext): RunMcpToolResponse {
    if (context !== undefined) this.storeContexts.push(context);
    return { ...stored.response, attachments: [] };
  }
  fail(): void {}
}
