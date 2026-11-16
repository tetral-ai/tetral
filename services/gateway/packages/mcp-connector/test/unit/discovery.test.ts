import { fixtureServerResolver } from "../fixtures/registered-server.js";
import { afterEach, expect, test } from "bun:test";
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { CallToolRequestSchema, ListToolsRequestSchema, McpError, ErrorCode } from "@modelcontextprotocol/sdk/types.js";
import type { ListToolsResult, ListToolsRequest, CallToolResult } from "@modelcontextprotocol/sdk/types.js";
import { Metadata, credentials } from "@grpc/grpc-js";
import { McpSDKClient } from "../../src/client.js";
import { SQLMcpCredentialResolver } from "../../src/credential.js";
import type { McpCredentialSQL } from "../../src/credential.js";
import { SQLMcpServerResolver } from "../../src/server-resolver.js";
import type { McpSDKClientOptions } from "../../src/client.js";
import { createMcpConnectorGrpcServer } from "../../src/server.js";
import { McpConnectorServiceShell } from "../../src/service.js";
import { InMemoryMcpIdempotencyStore } from "../../src/idempotency.js";
import { createJsonLogger } from "../../src/logger.js";
import { McpConnectorServiceClient, McpErrorKind } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";

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
  const written = createJsonLogger({ write: (line) => { logs.push(JSON.parse(line) as Record<string, unknown>); } });
  const client = new McpSDKClient({serverResolver: fixtureServerResolver,
    credentialResolver: { resolve: async () => credential, refresh: async () => credential },
    onToolsListChanged: async () => {},
    logger: { error: (record) => written.error(record) },
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
    logger: { info: () => {}, error: (record) => written.error(record) },
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

test("SDK discovery timeout names discovery while tool-call timeout still names the call", async () => {
  const f = fixture(() => { throw new McpError(ErrorCode.RequestTimeout, "synthetic timeout"); }, {}, async () => {
    throw new McpError(ErrorCode.RequestTimeout, "synthetic timeout");
  });
  await expect(f.client.listTools(identity)).rejects.toMatchObject({ code: "mcp_timeout", message: "MCP tool discovery timed out." });
  await expect(f.call("normal")).rejects.toMatchObject({ code: "mcp_timeout", message: "MCP tool call timed out." });
});

test("a rejected list request preserves its error without claiming a pagination violation", async () => {
  const f = fixture(() => { throw new McpError(ErrorCode.InvalidParams, "synthetic rejection"); });
  await expect(f.client.listTools(identity)).rejects.toMatchObject({ code: "mcp_invalid_input" });
  expect(f.requests).toHaveLength(1);
  expect(f.logs).toEqual([]);
});

test("catalog rejection before discovery does not claim an invalid page response", async () => {
  const f = fixture(() => ({ tools: [] }));
  await expect(f.client.listTools({ ...identity, mcpServerName: "unknown_test_server" })).rejects.toMatchObject({
    code: "mcp_invalid_input", message: "Unsupported configured fixture server.",
  });
  expect(f.requests).toHaveLength(0);
  expect(f.logs).toEqual([]);
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

test("JSON-RPC authentication-number errors discard partial discovery without token refresh", async () => {
  const f = fixture(params => {
    if (params?.cursor) throw new McpError(401, "application rejection");
    return {tools: [tool("partial")], nextCursor: "application_cursor"};
  });
  await expect(f.client.listTools(identity)).rejects.toMatchObject({code: 401});
  expect(f.requests.map(p => p?.cursor)).toEqual([undefined, "application_cursor"]);
  expect(f.client.connectionCount()).toBe(0);
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
  expect(f.logs).toContainEqual(expect.objectContaining({ "mcp.discovery.failure_reason": "repeated_cursor", "error.message": "MCP tool discovery failed (repeated_cursor)." }));
  expect(JSON.stringify(f.logs)).not.toContain("private_cursor_do_not_log");
});

// The database is an Engine dependency: its error holds no MCP server text.
for (const [name, options] of [
  ["the server resolver", (sql: McpCredentialSQL) => ({ serverResolver: new SQLMcpServerResolver(sql) })],
  ["the credential lookup", (sql: McpCredentialSQL) => ({ credentialResolver: new SQLMcpCredentialResolver(sql, "00".repeat(32)) })],
] as const) {
  test(`a database error of ${name} reaches the failure record as its cause`, async () => {
    const sql = (async () => []) as unknown as McpCredentialSQL;
    sql.begin = async () => { throw new Error("connection terminated unexpectedly"); };
    let contacted = false;
    const f = fixture(() => { contacted = true; return { tools: [] }; }, options(sql));
    const server = createMcpConnectorGrpcServer(f.shell);
    const port = await server.bind("127.0.0.1:0");
    cleanups.push(() => server.shutdown());
    const rpc = new McpConnectorServiceClient(`127.0.0.1:${port}`, credentials.createInsecure());
    cleanups.push(async () => rpc.close());
    const error = await new Promise<import("@grpc/grpc-js").ServiceError | null>((resolve) => rpc.listMcpTools(identity, new Metadata(), (error) => resolve(error)));
    expect(error).not.toBeNull();
    expect(contacted).toBe(false);
    expect(f.logs.filter((record) => record["error.code"] !== undefined).map((record) => [record.event, record["error.message"]])).toEqual([
      ["mcp_manifest_discovery_failed", "connection terminated unexpectedly"],
    ]);
  });

  test(`a database error of ${name} on a tool call reaches the failure record as its cause`, async () => {
    const sql = (async () => []) as unknown as McpCredentialSQL;
    sql.begin = async () => { throw new Error("connection terminated unexpectedly"); };
    let contacted = false;
    const f = fixture(() => { contacted = true; return { tools: [checked] }; }, options(sql), async () => { contacted = true; return { content: [] }; });
    const response = await runCheckedTool(f);
    expect(contacted).toBe(false);
    // The model is told only that the connector failed; the record names the database's error.
    expect(response).toMatchObject({ resultText: "MCP connector failed.", errorKind: McpErrorKind.MCP_ERROR_KIND_INTERNAL });
    expect(failureRecords(f)).toEqual([["mcpconnector.call", "MCP connector failed: connection terminated unexpectedly"]]);
  });
}

// A typed error's cause is the connector's own (its credential store and its
// decryption), never the MCP server's: the record names it after the sentence
// the model is told.
for (const [name, broken, resultText, errorKind, cause] of [
  ["the local decryption", "ciphertext", "MCP authentication failed after refresh.", McpErrorKind.MCP_ERROR_KIND_AUTHENTICATION_FAILED, "ciphertext is too short"],
  ["the refresh transaction", "transaction", "MCP credential refresh is temporarily unavailable.", McpErrorKind.MCP_ERROR_KIND_CONNECTION_FAILED, "connection terminated unexpectedly"],
  ["the credential UPDATE", "update", "MCP credential refresh is temporarily unavailable.", McpErrorKind.MCP_ERROR_KIND_CONNECTION_FAILED, "credential write unavailable"],
] as const) {
  test(`a failure of ${name} on a tool call reaches the failure record as its cause`, async () => {
    const sql = await expiredCredentialSQL(broken);
    const refreshed = async () => Response.json({ access_token: "new-access", refresh_token: "new-refresh", expires_in: 3600 });
    let contacted = false;
    const f = fixture(() => { contacted = true; return { tools: [checked] }; }, { credentialResolver: new SQLMcpCredentialResolver(sql, credentialKey, undefined, refreshed) }, async () => { contacted = true; return { content: [] }; });
    const response = await runCheckedTool(f);
    expect(contacted).toBe(false);
    expect(response).toMatchObject({ resultText, errorKind });
    expect(failureRecords(f)).toEqual([["mcpconnector.call", `${resultText.replace(/\.$/, "")}: ${cause}`]]);
  });
}

/** Runs the tool `checked` through the shell's RunMcpTool with the fixture's client, writing its records into the fixture's logs. */
async function runCheckedTool(f: ReturnType<typeof fixture>) {
  const written = createJsonLogger({ write: (line) => { f.logs.push(JSON.parse(line) as Record<string, unknown>); } });
  const shell = new McpConnectorServiceShell({
    client: f.client, ready: () => true,
    runtimeBindingTokenVerifier: { verify: () => true },
    idempotencyStore: new InMemoryMcpIdempotencyStore({ mcpServerName: "github", toolName: "checked", inputJson: "{}" }),
    authenticator: { authenticate: async () => ({ ok: true, serviceAccount: { namespace: "tetral", name: "runtime", podUid: "test" } }) },
    logger: { info: (record) => written.info(record), error: (record) => written.error(record) },
  });
  return await shell.runMcpTool({
    ...identity, sessionThreadId: "thr_test", toolUseEventId: "sevt_test",
    bindingId: "bind_test", bindingGeneration: 1, runtimeProcessId: "process-test", runtimeBindingToken: "test-only",
  }, new Metadata());
}

function failureRecords(f: ReturnType<typeof fixture>) {
  return f.logs.filter((record) => record["error.code"] !== undefined).map((record) => [record["event.kind"], record["error.message"]]);
}

const credentialKey = "00".repeat(32);

/**
 * A credential store holding one expired OAuth credential of the server, so
 * that resolving it refreshes it, broken at one place: its ciphertext, the
 * refresh's transaction, or the refresh's UPDATE.
 */
async function expiredCredentialSQL(broken: "ciphertext" | "transaction" | "update"): Promise<McpCredentialSQL> {
  const endpoint = "https://api.githubcopilot.com/mcp/";
  const auth = {
    type: "mcp_oauth", mcp_server_url: endpoint, access_token: "old-access", expires_at: "2000-01-01T00:00:00.000Z",
    refresh: { refresh_token: "old-refresh", client_id: "github-client", token_endpoint: "https://tokens.example/oauth" },
  };
  const key = await crypto.subtle.importKey("raw", Buffer.from(credentialKey, "hex"), "AES-GCM", false, ["encrypt"]);
  const nonce = new Uint8Array(12);
  const sealed = new Uint8Array(await crypto.subtle.encrypt({ name: "AES-GCM", iv: nonce }, key, new TextEncoder().encode(JSON.stringify(auth))));
  const row = {
    id: "cred_test", vault_id: "vlt_test", auth_public_json: JSON.stringify({ mcp_server_url: endpoint }),
    encrypted_auth: broken === "ciphertext" ? new Uint8Array([1]) : new Uint8Array([...nonce, ...sealed]),
  };
  const sql = (async (strings: TemplateStringsArray) => {
    const text = strings.join("?");
    if (text.includes("set_config")) return [];
    if (text.includes("UPDATE credentials") && broken === "update") throw new Error("credential write unavailable");
    return text.includes("UPDATE credentials") ? [] : [row];
  }) as McpCredentialSQL;
  let transactions = 0;
  sql.begin = async (fn) => {
    // The first transaction reads the credential; the second is the refresh's.
    if (++transactions === 2 && broken === "transaction") throw new Error("connection terminated unexpectedly");
    return await fn(sql);
  };
  return sql;
}

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
