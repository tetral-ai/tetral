import { afterEach, expect, test } from "bun:test";
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { CallToolRequestSchema, ListToolsRequestSchema, McpError, ErrorCode } from "@modelcontextprotocol/sdk/types.js";
import type { ListToolsResult, ListToolsRequest, CallToolResult } from "@modelcontextprotocol/sdk/types.js";
import { Metadata, credentials } from "@grpc/grpc-js";
import { McpSDKClient } from "../../src/client.js";
import type { McpSDKClientOptions } from "../../src/client.js";
import { createMcpConnectorGrpcServer } from "../../src/server.js";
import { McpConnectorServiceShell } from "../../src/service.js";
import { InMemoryMcpIdempotencyStore } from "../../src/idempotency.js";
import { McpConnectorServiceClient } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";

const identity = { workspaceId: "wksp_test", sessionId: "sesn_test", mcpServerName: "github" };
const credential = { ok: true, mode: "bearer", token: "test-only", tokenHash: "test", vaultId: "v", credentialId: "c" } as const;
const tool = (name: string, description = "Test tool") => ({ name, description, inputSchema: { type: "object" as const } });
const checked = { ...tool("checked"), outputSchema: { type: "object" as const, properties: { ok: { type: "boolean" } }, required: ["ok"] } };
const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => { for (const close of cleanups.splice(0).reverse()) await close(); });

function fixture(pages: (params: ListToolsRequest["params"]) => ListToolsResult | Promise<ListToolsResult>, options: Partial<McpSDKClientOptions> = {}, callHandler?: () => Promise<CallToolResult>) {
  const requests: Array<ListToolsRequest["params"]> = [];
  const calls: unknown[] = [];
  const logs: Record<string, unknown>[] = [];
  const client = new McpSDKClient({
    credentialResolver: { resolve: async () => credential, refresh: async () => credential },
    onToolsListChanged: async () => {},
    logger: { error: (record) => logs.push(record) },
    ...options,
    createTransport: () => {
      const server = new Server({ name: "test", version: "1" }, { capabilities: { tools: {} } });
      server.setRequestHandler(ListToolsRequestSchema, async (request) => { requests.push(request.params); return pages(request.params); });
      server.setRequestHandler(CallToolRequestSchema, async (request) => { calls.push(request.params); return callHandler ? await callHandler() : { content: [], structuredContent: { ok: "invalid" } }; });
      const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
      void server.connect(serverTransport);
      cleanups.push(() => server.close());
      return clientTransport;
    },
  });
  cleanups.push(() => client.closeAll());
  const call = (toolName: string, input = {}) => client.callTool({ ...identity, sessionThreadId: "thr_test", toolName, input });
  // Discovery does not use execution custody; exercise real shell auth and gRPC serialization.
  const shell = new McpConnectorServiceShell({
    client, ready: () => true,
    runtimeBindingTokenVerifier: { verify: () => false },
    idempotencyStore: new InMemoryMcpIdempotencyStore({ mcpServerName: "github", toolName: "checked", inputJson: "{}" }),
    authenticator: { authenticate: async () => ({ ok: true, serviceAccount: { namespace: "tetral", name: "bridge", podUid: "test" } }) },
    logger: { info: () => {}, error: (record) => logs.push(record) },
  });
  return { client, call, calls, requests, logs, shell };
}

test("complete SDK pagination retains first-page output validation and required-task guards", async () => {
  const f = fixture(params => params?.cursor === "page2"
    ? { tools: [tool("actions_list"), tool("actions_get"), tool("get_job_logs"), tool("actions_run_trigger")] }
    : { tools: [checked, { ...tool("task_only"), execution: { taskSupport: "required" } }], nextCursor: "page2" });
  expect((await f.client.listTools(identity)).map(t => t.name)).toEqual(["checked", "task_only", "actions_list", "actions_get", "get_job_logs", "actions_run_trigger"]);
  await expect(f.call("checked")).rejects.toMatchObject({ code: "mcp_invalid_input" });
  await expect(f.call("task_only")).rejects.toThrow("requires task-based execution");
  expect(f.calls).toHaveLength(1); // required-task guard rejects before sending a tool call
  await f.call("actions_run_trigger", { method: "run_workflow", owner: "tetral-ai", repo: "tetral", workflow_id: "ci.yaml", ref: "main" });
  expect(f.calls[1]).toMatchObject({ name: "actions_run_trigger", arguments: { method: "run_workflow", owner: "tetral-ai", repo: "tetral" } });
});

test("empty opaque cursor is followed and each complete listing restarts without a cursor", async () => {
  const f = fixture(params => params?.cursor === "" ? { tools: [tool("second")] } : { tools: [tool("first")], nextCursor: "" });
  expect(await f.client.listTools(identity)).toHaveLength(2);
  expect(await f.client.listTools(identity)).toHaveLength(2);
  expect(f.requests.map(p => p?.cursor)).toEqual([undefined, "", undefined, ""]);
});

test("failed later page preserves the previous SDK metadata and publishes no partial result", async () => {
  let refresh = false;
  const f = fixture(params => {
    if (!refresh) return { tools: [checked] };
    if (params?.cursor === "bad") throw new McpError(ErrorCode.InternalError, "synthetic failure");
    return { tools: [tool("partial")], nextCursor: "bad" };
  });
  await f.client.listTools(identity);
  refresh = true;
  await expect(f.client.listTools(identity)).rejects.toThrow("synthetic failure");
  await expect(f.call("checked")).rejects.toMatchObject({ code: "mcp_invalid_input" });
});

test("authentication restart discards old-session pages and starts cursorless", async () => {
  let rejected = false;
  const f = fixture(params => {
    if (rejected) return { tools: [tool("new_session")] };
    if (params?.cursor) { rejected = true; throw new McpError(401, "authentication rejected"); }
    return { tools: [tool("old_partial")], nextCursor: "old_session_cursor" };
  });
  expect((await f.client.listTools(identity)).map(t => t.name)).toEqual(["new_session"]);
  expect(f.requests.map(p => p?.cursor)).toEqual([undefined, "old_session_cursor", undefined]);
});

test("repeated cursor crosses actual gRPC as manifest_invalid with safe diagnostics", async () => {
  const f = fixture(() => ({ tools: [tool("first")], nextCursor: "private_cursor_do_not_log" }));
  const server = createMcpConnectorGrpcServer(f.shell);
  const port = await server.bind("127.0.0.1:0");
  cleanups.push(() => server.shutdown());
  const rpc = new McpConnectorServiceClient(`127.0.0.1:${port}`, credentials.createInsecure());
  cleanups.push(async () => rpc.close());
  const error = await new Promise<import("@grpc/grpc-js").ServiceError | null>((resolve) => rpc.listMcpTools(identity, new Metadata(), (error) => resolve(error)));
  expect(error?.code).toBe(9);
  expect(error?.metadata.get("tetral-mcp-failure-kind")).toEqual(["manifest_invalid"]);
  expect(f.logs).toContainEqual(expect.objectContaining({ "mcp.discovery.failure_reason": "repeated_cursor" }));
  expect(JSON.stringify(f.logs)).not.toContain("private_cursor_do_not_log");
});

for (const [reason, options] of [
  ["page_bound", { discoveryMaxPages: 2 }],
  ["tool_bound", { discoveryMaxTools: 2 }],
  ["byte_bound", { discoveryMaxBytes: 160 }],
] as const) {
  test(`enforces accumulated ${reason} through real SDK requests`, async () => {
    let pages = 0;
    const f = fixture(() => ({ tools: [tool(`tool_${++pages}`)], nextCursor: `page_${pages}` }), options);
    await expect(f.client.listTools(identity)).rejects.toMatchObject({ reason });
    expect(pages).toBeLessThanOrEqual(3);
  });
}

test("rejects a single oversized tool definition before publishing a manifest", async () => {
  const f = fixture(() => ({ tools: [tool("oversized", "x".repeat(512))] }), { discoveryMaxBytes: 256 });
  await expect(f.client.listTools(identity)).rejects.toMatchObject({ reason: "byte_bound" });
  expect(f.requests).toHaveLength(1);
});

test("all pages share a deadline and stopped discovery issues no later page requests", async () => {
  let page = 0;
  const f = fixture(async () => { await Bun.sleep(15); return { tools: [tool(`page_${++page}`)], nextCursor: String(page) }; }, { discoveryTimeoutMs: 40 });
  await expect(f.client.listTools(identity)).rejects.toMatchObject({ code: "mcp_timeout" });
  const atFailure = f.requests.length;
  await Bun.sleep(35);
  expect(f.requests).toHaveLength(atFailure);
  expect(atFailure).toBeLessThan(5);
});

test("gRPC cancellation stops pagination without canceling an in-flight tool call on the same SDK client", async () => {
  let block = false;
  let releaseCall!: () => void;
  let markCallStarted!: () => void;
  const blockedCall = new Promise<void>(resolve => { releaseCall = resolve; });
  const callStarted = new Promise<void>(resolve => { markCallStarted = resolve; });
  const f = fixture(async () => { if (block) await Bun.sleep(45); return { tools: [tool("normal")], ...(block ? { nextCursor: "next" } : {}) }; }, {}, async () => {
    markCallStarted();
    await blockedCall;
    return { content: [] };
  });
  await f.client.listTools(identity);
  block = true;
  const calling = f.call("normal");
  await callStarted;
  const server = createMcpConnectorGrpcServer(f.shell);
  const port = await server.bind("127.0.0.1:0");
  cleanups.push(() => server.shutdown());
  const rpc = new McpConnectorServiceClient(`127.0.0.1:${port}`, credentials.createInsecure());
  cleanups.push(async () => rpc.close());
  await new Promise<void>(resolve => rpc.listMcpTools(identity, new Metadata(), { deadline: Date.now() + 20 }, () => resolve()));
  const requests = f.requests.length;
  releaseCall();
  await expect(calling).resolves.toMatchObject({ content: [] });
  await Bun.sleep(55);
  expect(f.requests).toHaveLength(requests);
  expect(f.client.connectionCount()).toBe(1);
});
