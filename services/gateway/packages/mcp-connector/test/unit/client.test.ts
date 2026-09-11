import { describe, expect, test } from "bun:test";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { ErrorCode } from "@modelcontextprotocol/sdk/types.js";
import {
  MCP_CONNECT_TIMEOUT_MS,
  MCP_CREDENTIAL_RESOLUTION_TIMEOUT_MS,
  MCP_DISCOVERY_MAX_PAGES,
  MCP_DISCOVERY_MAX_TOOLS,
  MCP_RECONNECT_DELAYS_MS,
  MCP_RECONNECT_MAX_RETRIES,
  MCP_TOOLSETS_HEADER,
  McpSDKClient,
  mcpToolsListChangedFailureLogRecord,
  streamableHTTPTransportOptions,
} from "../../src/client.js";
import type { SDKClientLike } from "../../src/client.js";
import type { McpSDKClientOptions } from "../../src/client.js";
import type { GitHubMcpCredentialResolver } from "../../src/credential.js";

type RecordingSDKTool = Awaited<ReturnType<SDKClientLike["listTools"]>>["tools"][number] & {
  readonly enabled?: boolean | undefined;
};

describe("McpSDKClient", () => {
  test("carries the catalog toolset selection header beside bearer authorization", () => {
    const options = streamableHTTPTransportOptions({ token: "token-a", toolsets: "default,actions" });
    expect(options.requestInit).toEqual({
      headers: { Authorization: "Bearer token-a", [MCP_TOOLSETS_HEADER]: "default,actions" },
    });
    expect(streamableHTTPTransportOptions({ toolsets: "default,actions" }).requestInit).toEqual({
      headers: { [MCP_TOOLSETS_HEADER]: "default,actions" },
    });
    expect(streamableHTTPTransportOptions({}).requestInit).toEqual({});
  });

  test("pins Streamable HTTP reconnect backoff and retry budget", () => {
    const options = streamableHTTPTransportOptions({ token: "token-a" });
    const delays = Array.from({ length: MCP_RECONNECT_MAX_RETRIES }, (_, index) => {
      return Math.min(
        options.reconnectionOptions.initialReconnectionDelay * options.reconnectionOptions.reconnectionDelayGrowFactor ** index,
        options.reconnectionOptions.maxReconnectionDelay,
      );
    });

    expect(options.requestInit).toEqual({ headers: { Authorization: "Bearer token-a" } });
    expect(options.reconnectionOptions).toEqual({
      initialReconnectionDelay: 1000,
      reconnectionDelayGrowFactor: 4,
      maxReconnectionDelay: 16000,
      maxRetries: 3,
    });
    expect(delays).toEqual([...MCP_RECONNECT_DELAYS_MS]);
  });

  test("tools/list_changed failure logs carry shared correlation fields", () => {
    for (const failure of ["refresh_failed", "notify_failed"] as const) {
      expect(mcpToolsListChangedFailureLogRecord(validIdentity(), failure)).toMatchObject({
        event: `mcp_tools_list_changed_${failure}`,
        "event.kind": `mcp_tools_list_changed_${failure}`,
        operation: "mcp_manifest_refresh",
        component: "mcp-connector",
        "workspace.id": "wksp_1",
        "session.id": "sesn_1",
        mcp_server_name: "github",
        "error.class": "mcp_connection_failed",
        "error.code": "mcp_connection_failed",
        "error.message_safe": `mcp tools/list_changed ${failure === "refresh_failed" ? "refresh" : "notify"} failed`,
      });
    }
  });

  test("maps SDK reconnect errors to retrying and exhausted statuses", async () => {
    const cases = [
      {
        error: new Error("Failed to reconnect SSE stream: fake server still down"),
        retryStatus: "retrying",
      },
      {
        error: new Error("Maximum reconnection attempts (3) exceeded."),
        retryStatus: "exhausted",
      },
    ] as const;
    for (const tc of cases) {
      const sdk = new RecordingSDKClient();
      sdk.callToolError = tc.error;
      const client = new McpSDKClient({
        credentialResolver: new RotatingCredentialResolver(["token-a"]),
        onToolsListChanged: async () => undefined,
        createClient: () => sdk,
        createTransport: (input) => input,
        setTimer: fakeSetTimer,
        clearTimer: () => undefined,
      });

      await expect(client.callTool({
        ...validIdentity(),
        sessionThreadId: "thrd_1",
        toolName: "create_issue",
        input: {},
      })).rejects.toMatchObject({ code: "mcp_connection_failed", retryStatus: tc.retryStatus });
    }
  });

  test("Streamable HTTP reconnect attempts honor 1s, 4s, 16s on a dropped fake stream", async () => {
    const originalSetTimeout = globalThis.setTimeout;
    const originalClearTimeout = globalThis.clearTimeout;
    const delays: number[] = [];
    const retryTimestamps: number[] = [];
    const errors: string[] = [];
    let virtualNow = 0;
    let fetchCalls = 0;
    globalThis.setTimeout = ((callback: () => void, ms?: number) => {
      const delay = Number(ms ?? 0);
      delays.push(delay);
      virtualNow += delay;
      queueMicrotask(callback);
      return { unref: () => undefined } as unknown as ReturnType<typeof setTimeout>;
    }) as typeof setTimeout;
    globalThis.clearTimeout = (() => undefined) as typeof clearTimeout;
    try {
      const transport = new StreamableHTTPClientTransport(new URL("https://api.githubcopilot.com/mcp/"), {
        ...streamableHTTPTransportOptions({ token: "token-a" }),
        fetch: async () => {
          fetchCalls += 1;
          if (fetchCalls > 1) {
            retryTimestamps.push(virtualNow);
            throw new Error("fake server still down");
          }
          return new Response(new ReadableStream({
            start(controller) {
              controller.error(new Error("fake drop"));
            },
          }), {
            status: 200,
            headers: { "content-type": "text/event-stream" },
          });
        },
      });
      transport.onerror = (error) => {
        errors.push(error.message);
      };

      await transport.start();
      await (transport as unknown as {
        _startOrAuthSse(options: { readonly resumptionToken: string | undefined }): Promise<void>;
      })._startOrAuthSse({ resumptionToken: undefined });
      await flushMicrotasks(20);

      expect(fetchCalls).toBe(4);
      expect(delays).toEqual([...MCP_RECONNECT_DELAYS_MS]);
      expect(retryTimestamps).toEqual([1000, 5000, 21000]);
      expect(errors.some((message) => message === "Maximum reconnection attempts (3) exceeded.")).toBe(true);
      await transport.close();
    } finally {
      globalThis.setTimeout = originalSetTimeout;
      globalThis.clearTimeout = originalClearTimeout;
    }
  });

  test("terminal reconnect exhaustion settles every in-flight call once and evicts the dead client", async () => {
    const callGate = deferred<void>();
    const listGate = deferred<void>();
    const clients: RecordingSDKClient[] = [];
    const client = new McpSDKClient({
      credentialResolver: new RotatingCredentialResolver(["token-a"]),
      onToolsListChanged: async () => undefined,
      createClient: () => {
        const sdk = new RecordingSDKClient();
        sdk.callToolGate = callGate.promise;
        sdk.listToolsGate = listGate.promise;
        clients.push(sdk);
        return sdk;
      },
      createTransport: (input) => input,
      setTimer: fakeSetTimer,
      clearTimer: () => undefined,
    });

    const tool = client.callTool({
      ...validIdentity(),
      sessionThreadId: "thrd_1",
      toolName: "create_issue",
      input: {},
    });
    const list = client.listTools(validIdentity());
    let outcomes: PromiseSettledResult<unknown>[] | undefined;
    const settled = Promise.allSettled([tool, list]).then((result) => {
      outcomes = result;
    });
    try {
      await until(() => clients.length === 1 && clients[0]?.callToolOptions.length === 1 && clients[0]?.listToolsOptions.length === 1);
      clients[0]?.onerror?.(new Error("Maximum reconnection attempts (3) exceeded."));
      await flushMicrotasks(10);

      expect(outcomes).toHaveLength(2);
      for (const outcome of outcomes ?? []) {
        expect(outcome.status).toBe("rejected");
        if (outcome.status === "rejected") {
          expect(outcome.reason).toMatchObject({ code: "mcp_connection_failed", retryStatus: "exhausted" });
        }
      }
      expect(clients[0]?.closeCount).toBe(1);
      expect(client.connectionCount()).toBe(0);

      clients[0]?.onerror?.(new Error("Maximum reconnection attempts (3) exceeded."));
      callGate.resolve();
      listGate.resolve();
      await flushMicrotasks(10);
      expect(clients[0]?.closeCount).toBe(1);

      await expect(client.listTools(validIdentity())).resolves.toHaveLength(1);
      expect(clients).toHaveLength(2);
      expect(client.connectionCount()).toBe(1);
    } finally {
      callGate.resolve();
      listGate.resolve();
      await settled;
    }
  });

  test("opens a catalog transport with bearer auth and maps tool definitions", async () => {
    const credentials = new RotatingCredentialResolver(["token-a"]);
    const transports: unknown[] = [];
    const clients: RecordingSDKClient[] = [];
    const client = new McpSDKClient({
      credentialResolver: credentials,
      onToolsListChanged: async () => undefined,
      createClient: () => {
        const sdk = new RecordingSDKClient();
        clients.push(sdk);
        return sdk;
      },
      createTransport: (input) => {
        transports.push(input);
        return input;
      },
      setTimer: fakeSetTimer,
      clearTimer: () => undefined,
    });

    const tools = await client.listTools(validIdentity());

    expect(tools).toEqual([{ name: "create_issue", description: "Create an issue.", inputSchema: { type: "object" } }]);
    expect(transports).toEqual([{ url: new URL("https://api.githubcopilot.com/mcp/"), token: "token-a", toolsets: "default,actions" }]);
    expect(clients).toHaveLength(1);
    expect(clients[0]?.connects).toBe(1);
  });

  test("sends the catalog toolset selection on every newly created transport, including credential replacement", async () => {
    const credentials = new RotatingCredentialResolver(["token-a", "token-b"]);
    const transports: Array<{ readonly url: URL; readonly token?: string | undefined; readonly toolsets?: string | undefined }> = [];
    const client = new McpSDKClient({
      credentialResolver: credentials,
      onToolsListChanged: async () => undefined,
      createClient: () => new RecordingSDKClient(),
      createTransport: (input) => {
        transports.push(input);
        return input;
      },
      setTimer: fakeSetTimer,
      clearTimer: () => undefined,
    });

    await client.listTools(validIdentity());
    await client.listTools(validIdentity());

    expect(transports).toEqual([
      { url: new URL("https://api.githubcopilot.com/mcp/"), token: "token-a", toolsets: "default,actions" },
      { url: new URL("https://api.githubcopilot.com/mcp/"), token: "token-b", toolsets: "default,actions" },
    ]);
    expect(client.connectionCount()).toBe(1);
  });

  test("preserves disabled tool metadata from the SDK adapter", async () => {
    const clients: RecordingSDKClient[] = [];
    const client = new McpSDKClient({
      credentialResolver: new RotatingCredentialResolver(["token-a"]),
      onToolsListChanged: async () => undefined,
      createClient: () => {
        const sdk = new RecordingSDKClient();
        sdk.tools = [
          { name: "create_issue", description: "Create an issue.", inputSchema: { type: "object" as const } },
          { name: "disabled_tool", description: "Hidden.", inputSchema: { type: "object" as const }, enabled: false },
        ];
        clients.push(sdk);
        return sdk;
      },
      createTransport: (input) => input,
      setTimer: fakeSetTimer,
      clearTimer: () => undefined,
    });

    const tools = await client.listTools(validIdentity());

    expect(tools).toEqual([
      { name: "create_issue", description: "Create an issue.", inputSchema: { type: "object" } },
      { name: "disabled_tool", description: "Hidden.", inputSchema: { type: "object" }, enabled: false },
    ]);
  });

  test("token rotation closes the old SDK client and creates one connection for the new token", async () => {
    const credentials = new RotatingCredentialResolver(["token-a", "token-b"]);
    const clients: RecordingSDKClient[] = [];
    const client = new McpSDKClient({
      credentialResolver: credentials,
      onToolsListChanged: async () => undefined,
      createClient: () => {
        const sdk = new RecordingSDKClient();
        clients.push(sdk);
        return sdk;
      },
      createTransport: (input) => input,
      setTimer: fakeSetTimer,
      clearTimer: () => undefined,
    });

    await client.listTools(validIdentity());
    await client.listTools(validIdentity());

    expect(clients).toHaveLength(2);
    expect(clients[0]?.closed).toBe(true);
    expect(clients[1]?.closed).toBe(false);
    expect(client.connectionCount()).toBe(1);
  });

  test("concurrent calls with the same cache key share one connection establishment", async () => {
    const connectGate = deferred<void>();
    const clients: RecordingSDKClient[] = [];
    const client = new McpSDKClient({
      credentialResolver: new RotatingCredentialResolver(["token-a"]),
      onToolsListChanged: async () => undefined,
      createClient: () => {
        const sdk = new RecordingSDKClient();
        sdk.connectGate = connectGate.promise;
        clients.push(sdk);
        return sdk;
      },
      createTransport: (input) => input,
      setTimer: fakeSetTimer,
      clearTimer: () => undefined,
    });

    const first = client.listTools(validIdentity());
    const second = client.listTools(validIdentity());
    await until(() => clients.length === 1 && clients[0]?.connects === 1);

    expect(clients).toHaveLength(1);
    connectGate.resolve();
    await expect(Promise.all([first, second])).resolves.toHaveLength(2);
    expect(clients).toHaveLength(1);
    expect(client.connectionCount()).toBe(1);
  });

  test("failed connection establishment releases the single-flight reservation", async () => {
    const clients: RecordingSDKClient[] = [];
    const client = new McpSDKClient({
      credentialResolver: new RotatingCredentialResolver(["token-a"]),
      onToolsListChanged: async () => undefined,
      createClient: () => {
        const sdk = new RecordingSDKClient();
        if (clients.length === 0) {
          sdk.connectError = new Error("connect failed");
        }
        clients.push(sdk);
        return sdk;
      },
      createTransport: (input) => input,
      setTimer: fakeSetTimer,
      clearTimer: () => undefined,
    });

    await expect(client.listTools(validIdentity())).rejects.toThrow("connect failed");
    await expect(client.listTools(validIdentity())).resolves.toHaveLength(1);
    expect(clients).toHaveLength(2);
    expect(client.connectionCount()).toBe(1);
  });

  test("credential_required failures surface before network I/O", async () => {
    const client = new McpSDKClient({
      credentialResolver: {
        resolve: async () => ({ ok: false, error: "credential_required" }),
        refresh: async () => ({ ok: false, error: "credential_required" }),
      },
      onToolsListChanged: async () => undefined,
      createClient: () => {
        throw new Error("network should not be reached");
      },
      createTransport: () => {
        throw new Error("network should not be reached");
      },
      setTimer: fakeSetTimer,
      clearTimer: () => undefined,
    });

    await expect(client.callTool({
      ...validIdentity(),
      sessionThreadId: "thrd_1",
      toolName: "create_issue",
      input: {},
    })).rejects.toMatchObject({
      code: "mcp_credential_required",
      message: "MCP server github requires a configured credential.",
      retryStatus: "terminal",
    });
  });

  test("reports transient OAuth refresh unavailability without retrying the rejected credential", async () => {
    let refreshes = 0;
    const clients: RecordingSDKClient[] = [];
    const client = new McpSDKClient({
      credentialResolver: {
        resolve: async () => ({
          ok: true, mode: "bearer", token: "rejected-token", tokenHash: "rejected-token",
          vaultId: "vlt_1", credentialId: "cred_1",
        }),
        refresh: async () => {
          refreshes += 1;
          return { ok: false as const, error: "refresh_unavailable" as const };
        },
      },
      onToolsListChanged: async () => undefined,
      createClient: () => {
        const sdk = new RecordingSDKClient();
        sdk.callToolError = Object.assign(new Error("HTTP 401"), { code: 401 });
        clients.push(sdk);
        return sdk;
      },
      createTransport: (input) => input,
      setTimer: fakeSetTimer,
      clearTimer: () => undefined,
    });

    await expect(client.callTool({
      ...validIdentity(),
      sessionThreadId: "thrd_1",
      toolName: "create_issue",
      input: {},
    })).rejects.toMatchObject({
      code: "mcp_connection_failed",
      retryStatus: "terminal",
    });
    expect(refreshes).toBe(1);
    expect(clients).toHaveLength(1);
  });

  test("rejects non-bearer resolver output before network I/O", async () => {
    const resolver = {
      resolve: async () => ({ ok: true, mode: "anonymous", tokenHash: "anonymous" }),
      refresh: async () => ({ ok: false, error: "credential_required" }),
    } as unknown as GitHubMcpCredentialResolver;
    const client = new McpSDKClient({
      credentialResolver: resolver,
      onToolsListChanged: async () => undefined,
      createClient: () => {
        throw new Error("network should not be reached");
      },
      createTransport: () => {
        throw new Error("network should not be reached");
      },
      setTimer: fakeSetTimer,
      clearTimer: () => undefined,
    });

    await expect(client.listTools(validIdentity())).rejects.toMatchObject({
      code: "mcp_authentication_failed",
      retryStatus: "terminal",
    });
    expect(client.connectionCount()).toBe(0);
  });

  test("refreshes once and retries when an established MCP call returns 401", async () => {
    const credentials = new RotatingCredentialResolver(["token-a"], ["token-b"]);
    const clients: RecordingSDKClient[] = [];
    const client = new McpSDKClient({
      credentialResolver: credentials,
      onToolsListChanged: async () => undefined,
      createClient: () => {
        const sdk = new RecordingSDKClient();
        if (clients.length === 0) {
          sdk.callToolError = Object.assign(new Error("HTTP 401"), { code: 401 });
        }
        clients.push(sdk);
        return sdk;
      },
      createTransport: (input) => input,
      setTimer: fakeSetTimer,
      clearTimer: () => undefined,
    });

    const result = await client.callTool({
      ...validIdentity(),
      sessionThreadId: "thrd_1",
      toolName: "create_issue",
      input: {},
    });

    expect(result).toEqual({ content: [{ type: "text", text: "ok" }], refreshTriggered: true });
    expect(credentials.refreshes).toBe(1);
    expect(credentials.refreshInputs[0]).toMatchObject({
      vaultId: "vlt_1",
      credentialId: "cred_1",
      previousTokenHash: "token-a",
    });
    expect(clients).toHaveLength(2);
    expect(clients[0]?.closed).toBe(true);
    expect(client.connectionCount()).toBe(1);
  });

  test("carries the selected credential identity through an initialize-time auth refresh", async () => {
    const credentials = new RotatingCredentialResolver(["token-a"], ["token-b"]);
    let clientCount = 0;
    const client = new McpSDKClient({
      credentialResolver: credentials,
      onToolsListChanged: async () => undefined,
      createClient: () => {
        const sdk = new RecordingSDKClient();
        if (clientCount === 0) {
          sdk.connectError = Object.assign(new Error("HTTP 401"), { code: 401 });
        }
        clientCount += 1;
        return sdk;
      },
      createTransport: (input) => input,
      setTimer: fakeSetTimer,
      clearTimer: () => undefined,
    });

    await expect(client.listTools(validIdentity())).resolves.toHaveLength(1);
    expect(credentials.refreshInputs).toHaveLength(1);
    expect(credentials.refreshInputs[0]).toMatchObject({
      vaultId: "vlt_1",
      credentialId: "cred_1",
      previousTokenHash: "token-a",
    });
  });

  test("does not reuse a same-token connection across credential identities", async () => {
    let resolutions = 0;
    const clients: RecordingSDKClient[] = [];
    const client = new McpSDKClient({
      credentialResolver: {
        resolve: async () => {
          resolutions += 1;
          return {
            ok: true as const,
            mode: "bearer" as const,
            token: "shared-token",
            tokenHash: "shared-token-hash",
            vaultId: "vlt_1",
            credentialId: resolutions === 1 ? "cred_a" : "cred_b",
          };
        },
        refresh: async () => ({ ok: false as const, error: "refresh_failed" as const }),
      },
      onToolsListChanged: async () => undefined,
      createClient: () => {
        const sdk = new RecordingSDKClient();
        clients.push(sdk);
        return sdk;
      },
      createTransport: (input) => input,
      setTimer: fakeSetTimer,
      clearTimer: () => undefined,
    });

    await expect(client.listTools(validIdentity())).resolves.toHaveLength(1);
    await expect(client.listTools(validIdentity())).resolves.toHaveLength(1);

    expect(clients).toHaveLength(2);
    expect(clients[0]?.closed).toBe(true);
    expect(clients[1]?.connects).toBe(1);
    expect(client.connectionCount()).toBe(1);
  });

  test("surfaces proactive OAuth refresh during initial connection", async () => {
    const client = new McpSDKClient({
      credentialResolver: {
        resolve: async () => ({ ok: true, mode: "bearer", token: "token-b", tokenHash: "token-b", vaultId: "vlt_1", credentialId: "cred_1", refreshTriggered: true }),
        refresh: async () => ({ ok: false, error: "refresh_failed" }),
      },
      onToolsListChanged: async () => undefined,
      createClient: () => new RecordingSDKClient(),
      createTransport: (input) => input,
      setTimer: fakeSetTimer,
      clearTimer: () => undefined,
    });

    const result = await client.callTool({
      ...validIdentity(),
      sessionThreadId: "thrd_1",
      toolName: "create_issue",
      input: {},
    });

    expect(result).toEqual({ content: [{ type: "text", text: "ok" }], refreshTriggered: true });
  });

  test("locked-row credential reuse does not mark the call as refreshed", async () => {
    const client = new McpSDKClient({
      credentialResolver: {
        resolve: async () => ({ ok: true, mode: "bearer", token: "already-rotated", tokenHash: "already-rotated", vaultId: "vlt_1", credentialId: "cred_1" }),
        refresh: async () => ({ ok: false, error: "refresh_failed" }),
      },
      onToolsListChanged: async () => undefined,
      createClient: () => new RecordingSDKClient(),
      createTransport: (input) => input,
      setTimer: fakeSetTimer,
      clearTimer: () => undefined,
    });

    const result = await client.callTool({
      ...validIdentity(),
      sessionThreadId: "thrd_1",
      toolName: "create_issue",
      input: {},
    });

    expect(result).toEqual({ content: [{ type: "text", text: "ok" }], refreshTriggered: false });
  });

  test("surfaces failed proactive refresh during initial connection", async () => {
    const client = new McpSDKClient({
      credentialResolver: {
        resolve: async () => ({ ok: false, error: "refresh_failed" }),
        refresh: async () => ({ ok: false, error: "refresh_failed" }),
      },
      onToolsListChanged: async () => undefined,
      createClient: () => {
        throw new Error("network should not be reached");
      },
      createTransport: () => {
        throw new Error("network should not be reached");
      },
      setTimer: fakeSetTimer,
      clearTimer: () => undefined,
    });

    await expect(client.callTool({
      ...validIdentity(),
      sessionThreadId: "thrd_1",
      toolName: "create_issue",
      input: {},
    })).rejects.toMatchObject({ code: "mcp_authentication_failed", retryStatus: "terminal" });
  });

  test("refreshes once and retries when an established MCP call returns 403", async () => {
    const credentials = new RotatingCredentialResolver(["token-a"], ["token-b"]);
    const client = new McpSDKClient({
      credentialResolver: credentials,
      onToolsListChanged: async () => undefined,
      createClient: () => {
        const sdk = new RecordingSDKClient();
        if (credentials.refreshes === 0) {
          sdk.callToolError = Object.assign(new Error("HTTP 403"), { code: 403 });
        }
        return sdk;
      },
      createTransport: (input) => input,
      setTimer: fakeSetTimer,
      clearTimer: () => undefined,
    });

    const result = await client.callTool({
      ...validIdentity(),
      sessionThreadId: "thrd_1",
      toolName: "create_issue",
      input: {},
    });

    expect(result).toEqual({ content: [{ type: "text", text: "ok" }], refreshTriggered: true });
    expect(credentials.refreshes).toBe(1);
  });

  test("second 401 after refresh is terminal", async () => {
    const credentials = new RotatingCredentialResolver(["token-a"], ["token-b"]);
    const client = new McpSDKClient({
      credentialResolver: credentials,
      onToolsListChanged: async () => undefined,
      createClient: () => {
        const sdk = new RecordingSDKClient();
        sdk.callToolError = Object.assign(new Error("HTTP 401"), { code: 401 });
        return sdk;
      },
      createTransport: (input) => input,
      setTimer: fakeSetTimer,
      clearTimer: () => undefined,
    });

    await expect(client.callTool({
      ...validIdentity(),
      sessionThreadId: "thrd_1",
      toolName: "create_issue",
      input: {},
    })).rejects.toMatchObject({ code: "mcp_authentication_failed", retryStatus: "terminal" });
    expect(credentials.refreshes).toBe(1);
  });

  test("surfaces a forced refresh that cannot mint a replacement", async () => {
    const client = new McpSDKClient({
      credentialResolver: {
        resolve: async () => ({ ok: true, mode: "bearer", token: "token-a", tokenHash: "token-a", vaultId: "vlt_1", credentialId: "cred_1" }),
        refresh: async () => ({ ok: false, error: "refresh_failed" }),
      },
      onToolsListChanged: async () => undefined,
      createClient: () => {
        const sdk = new RecordingSDKClient();
        sdk.callToolError = Object.assign(new Error("HTTP 401"), { code: 401 });
        return sdk;
      },
      createTransport: (input) => input,
      setTimer: fakeSetTimer,
      clearTimer: () => undefined,
    });

    await expect(client.callTool({
      ...validIdentity(),
      sessionThreadId: "thrd_1",
      toolName: "create_issue",
      input: {},
    })).rejects.toMatchObject({ code: "mcp_authentication_failed", retryStatus: "terminal" });
  });

  test("closes an idle cached session and leaves connection cache size zero", async () => {
    const timer = controlledTimer();
    const clients: RecordingSDKClient[] = [];
    const client = new McpSDKClient({
      credentialResolver: new RotatingCredentialResolver(["token-a"]),
      onToolsListChanged: async () => undefined,
      createClient: () => {
        const sdk = new RecordingSDKClient();
        clients.push(sdk);
        return sdk;
      },
      createTransport: (input) => input,
      idleTimeoutMs: 7,
      setTimer: timer.setTimer,
      clearTimer: timer.clearTimer,
    });

    await client.listTools(validIdentity());
    expect(client.connectionCount()).toBe(1);
    expect(timer.delayMs).toBe(7);
    expect(timer.clearCount).toBe(1);

    timer.fire();
    await Promise.resolve();

    expect(clients).toHaveLength(1);
    expect(clients[0]?.closed).toBe(true);
    expect(client.connectionCount()).toBe(0);
    expect(timer.clearCount).toBe(2);
  });

  test("passes the bounded call timeout and maps SDK request timeout to mcp_timeout", async () => {
    const sdk = new RecordingSDKClient();
    sdk.callToolError = Object.assign(new Error("request timed out"), { code: ErrorCode.RequestTimeout });
    const client = new McpSDKClient({
      credentialResolver: new RotatingCredentialResolver(["token-a"]),
      onToolsListChanged: async () => undefined,
      createClient: () => sdk,
      createTransport: (input) => input,
      callTimeoutMs: 1234,
      setTimer: fakeSetTimer,
      clearTimer: () => undefined,
    });

    await expect(client.callTool({
      ...validIdentity(),
      sessionThreadId: "thrd_1",
      toolName: "create_issue",
      input: {},
    })).rejects.toMatchObject({ code: "mcp_timeout", retryStatus: undefined });
    expect(sdk.callToolOptions).toEqual([{ timeout: 1234 }]);
  });

  test("bounds credential resolution and classifies its deadline as mcp_timeout", async () => {
    const sdk = new RecordingSDKClient();
    const client = new McpSDKClient({
      credentialResolver: {
        resolve: async () => await new Promise<never>(() => undefined),
        refresh: async () => ({ ok: false, error: "refresh_failed" }),
      },
      onToolsListChanged: async () => undefined,
      createClient: () => sdk,
      createTransport: (input) => input,
      credentialTimeoutMs: 1,
      setTimer: fakeSetTimer,
      clearTimer: () => undefined,
    });

    await expect(client.listTools(validIdentity())).rejects.toMatchObject({
      code: "mcp_timeout",
      retryStatus: undefined,
    });
    expect(sdk.connects).toBe(0);
    expect(MCP_CREDENTIAL_RESOLUTION_TIMEOUT_MS).toBe(15_000);
  });

  test("overrides the SDK initialize timeout and classifies connect expiry as mcp_timeout", async () => {
    const sdk = new RecordingSDKClient();
    sdk.connectGate = new Promise<void>(() => undefined);
    const client = new McpSDKClient({
      credentialResolver: new RotatingCredentialResolver(["token-a"]),
      onToolsListChanged: async () => undefined,
      createClient: () => sdk,
      createTransport: (input) => input,
      connectTimeoutMs: 1,
      setTimer: fakeSetTimer,
      clearTimer: () => undefined,
    });

    await expect(client.listTools(validIdentity())).rejects.toMatchObject({
      code: "mcp_timeout",
      retryStatus: undefined,
    });
    expect(sdk.connectOptions).toHaveLength(1);
    expect(sdk.connectOptions[0]?.timeout).toBe(1);
    expect(sdk.connectOptions[0]?.signal?.aborted).toBe(true);
    expect(sdk.closed).toBe(true);
    expect(MCP_CONNECT_TIMEOUT_MS).toBe(10_000);
  });

  test("maps SDK JSON-RPC invalid params rejections to mcp_invalid_input", async () => {
    const sdk = new RecordingSDKClient();
    sdk.callToolError = Object.assign(new Error("Invalid params"), { code: ErrorCode.InvalidParams });
    const credentials = new RotatingCredentialResolver(["token-a"], ["token-b"]);
    const client = new McpSDKClient({
      credentialResolver: credentials,
      onToolsListChanged: async () => undefined,
      createClient: () => sdk,
      createTransport: (input) => input,
      setTimer: fakeSetTimer,
      clearTimer: () => undefined,
    });

    await expect(client.callTool({
      ...validIdentity(),
      sessionThreadId: "thrd_1",
      toolName: "create_issue",
      input: { title: 7 },
    })).rejects.toMatchObject({ code: "mcp_invalid_input", retryStatus: undefined });
    expect(credentials.refreshes).toBe(0);
    expect(sdk.callToolOptions).toHaveLength(1);
  });
});

describe("McpSDKClient discovery pagination", () => {
  test("pins the discovery page and accumulated-tool bounds", () => {
    expect(MCP_DISCOVERY_MAX_PAGES).toBe(100);
    expect(MCP_DISCOVERY_MAX_TOOLS).toBe(1024);
  });

  test("composes one complete manifest across pages with Actions tools on later pages", async () => {
    const sdk = new RecordingSDKClient();
    sdk.pagesByCursor = {
      "": {
        tools: [
          { name: "create_issue", description: "Create an issue.", inputSchema: { type: "object" as const } },
          { name: "create_pull_request", description: "Create a pull request.", inputSchema: { type: "object" as const } },
        ],
        nextCursor: "cursor-2",
      },
      "cursor-2": {
        tools: [
          { name: "actions_list", description: "List workflows, runs, jobs, and artifacts.", inputSchema: { type: "object" as const } },
          { name: "actions_get", description: "Read workflow, run, and job details.", inputSchema: { type: "object" as const } },
        ],
        nextCursor: "cursor-3",
      },
      "cursor-3": {
        tools: [
          { name: "get_job_logs", description: "Read job logs.", inputSchema: { type: "object" as const } },
          { name: "actions_run_trigger", description: "Trigger, rerun, or cancel runs.", inputSchema: { type: "object" as const } },
        ],
      },
    };
    const client = pagedTestClient(sdk);

    const tools = await client.listTools(validIdentity());

    expect(tools.map((tool) => tool.name)).toEqual([
      "create_issue",
      "create_pull_request",
      "actions_list",
      "actions_get",
      "get_job_logs",
      "actions_run_trigger",
    ]);
    expect(sdk.listToolsParams).toEqual([undefined, { cursor: "cursor-2" }, { cursor: "cursor-3" }]);
    expect(sdk.listToolsOptions.map((options) => options?.timeout)).toEqual([120_000, 120_000, 120_000]);
  });

  test("starts every listing cursorless, including a re-list after the previous pagination", async () => {
    const sdk = new RecordingSDKClient();
    sdk.pagesByCursor = {
      "": {
        tools: [{ name: "create_issue", description: "Create an issue.", inputSchema: { type: "object" as const } }],
        nextCursor: "cursor-2",
      },
      "cursor-2": {
        tools: [{ name: "actions_list", description: "List workflows and runs.", inputSchema: { type: "object" as const } }],
      },
    };
    const client = pagedTestClient(sdk);

    await expect(client.listTools(validIdentity())).resolves.toHaveLength(2);
    await expect(client.listTools(validIdentity())).resolves.toHaveLength(2);

    expect(sdk.listToolsParams).toEqual([undefined, { cursor: "cursor-2" }, undefined, { cursor: "cursor-2" }]);
  });

  test("rejects the whole listing when a later page fails and returns no partial manifest", async () => {
    const sdk = new RecordingSDKClient();
    sdk.pagesByCursor = {
      "": {
        tools: [{ name: "create_issue", description: "Create an issue.", inputSchema: { type: "object" as const } }],
        nextCursor: "cursor-2",
      },
      "cursor-2": { tools: [], error: new Error("page two unavailable") },
    };
    const client = pagedTestClient(sdk);

    await expect(client.listTools(validIdentity())).rejects.toThrow("page two unavailable");
    expect(sdk.listToolsParams).toEqual([undefined, { cursor: "cursor-2" }]);
  });

  test("rejects a repeated cursor terminally and logs the violation without credential material", async () => {
    const records: Record<string, unknown>[] = [];
    const sdk = new RecordingSDKClient();
    sdk.pagesByCursor = {
      "": {
        tools: [{ name: "create_issue", description: "Create an issue.", inputSchema: { type: "object" as const } }],
        nextCursor: "cursor-2",
      },
      "cursor-2": {
        tools: [{ name: "actions_list", description: "List workflows and runs.", inputSchema: { type: "object" as const } }],
        nextCursor: "cursor-2",
      },
    };
    const client = pagedTestClient(sdk, {}, records);

    await expect(client.listTools(validIdentity())).rejects.toMatchObject({
      code: "mcp_connection_failed",
      retryStatus: "terminal",
    });

    expect(records).toContainEqual(expect.objectContaining({
      event: "mcp_discovery_pagination_failed",
      "event.kind": "mcp_discovery_pagination_failed",
      operation: "mcp_manifest_list",
      component: "mcp-connector",
      "workspace.id": "wksp_1",
      "session.id": "sesn_1",
      mcp_server_name: "github",
      "mcp.discovery.failure_reason": "repeated_cursor",
      "error.class": "mcp_connection_failed",
    }));
    expect(JSON.stringify(records)).not.toContain("token-a");
  });

  test("bounds the number of pages one listing follows", async () => {
    const records: Record<string, unknown>[] = [];
    const sdk = new RecordingSDKClient();
    sdk.pagesByCursor = {
      "": { tools: [{ name: "tool_0", description: "", inputSchema: { type: "object" as const } }], nextCursor: "cursor-1" },
      "cursor-1": { tools: [{ name: "tool_1", description: "", inputSchema: { type: "object" as const } }], nextCursor: "cursor-2" },
      "cursor-2": { tools: [{ name: "tool_2", description: "", inputSchema: { type: "object" as const } }], nextCursor: "cursor-3" },
    };
    const client = pagedTestClient(sdk, { discoveryMaxPages: 3 }, records);

    await expect(client.listTools(validIdentity())).rejects.toMatchObject({
      code: "mcp_connection_failed",
      retryStatus: "terminal",
    });

    expect(sdk.listToolsParams).toEqual([undefined, { cursor: "cursor-1" }, { cursor: "cursor-2" }]);
    expect(records).toContainEqual(expect.objectContaining({ "mcp.discovery.failure_reason": "page_bound" }));
  });

  test("bounds the accumulated tool count across pages", async () => {
    const records: Record<string, unknown>[] = [];
    const sdk = new RecordingSDKClient();
    sdk.pagesByCursor = {
      "": {
        tools: [
          { name: "tool_0", description: "", inputSchema: { type: "object" as const } },
          { name: "tool_1", description: "", inputSchema: { type: "object" as const } },
        ],
        nextCursor: "cursor-1",
      },
      "cursor-1": {
        tools: [
          { name: "tool_2", description: "", inputSchema: { type: "object" as const } },
          { name: "tool_3", description: "", inputSchema: { type: "object" as const } },
        ],
      },
    };
    const client = pagedTestClient(sdk, { discoveryMaxTools: 3 }, records);

    await expect(client.listTools(validIdentity())).rejects.toMatchObject({
      code: "mcp_connection_failed",
      retryStatus: "terminal",
    });

    expect(records).toContainEqual(expect.objectContaining({ "mcp.discovery.failure_reason": "tool_bound" }));
  });

  test("executes an Actions trigger call through the same MCP route as any other tool", async () => {
    const sdk = new RecordingSDKClient();
    const client = pagedTestClient(sdk);

    const result = await client.callTool({
      ...validIdentity(),
      sessionThreadId: "thrd_1",
      toolName: "actions_run_trigger",
      input: { action: "trigger", workflow_id: "ci.yaml", ref: "main" },
    });

    expect(result).toEqual({ content: [{ type: "text", text: "ok" }], refreshTriggered: false });
    expect(sdk.callToolParams).toEqual([
      { name: "actions_run_trigger", arguments: { action: "trigger", workflow_id: "ci.yaml", ref: "main" } },
    ]);
  });
});

function pagedTestClient(
  sdk: RecordingSDKClient,
  overrides: Partial<McpSDKClientOptions> = {},
  records?: Record<string, unknown>[],
): McpSDKClient {
  return new McpSDKClient({
    credentialResolver: new RotatingCredentialResolver(["token-a"]),
    onToolsListChanged: async () => undefined,
    createClient: () => sdk,
    createTransport: (input) => input,
    setTimer: fakeSetTimer,
    clearTimer: () => undefined,
    ...(records === undefined ? {} : { logger: { error: (record) => { records.push({ ...record }); } } }),
    ...overrides,
  });
}

function validIdentity() {
  return {
    workspaceId: "wksp_1",
    sessionId: "sesn_1",
    mcpServerName: "github",
  };
}

function fakeSetTimer(callback: () => void, _ms: number): ReturnType<typeof setTimeout> {
  void callback;
  return setTimeout(() => undefined, 60_000);
}

async function flushMicrotasks(count: number) {
  for (let index = 0; index < count; index += 1) {
    await Promise.resolve();
  }
}

function controlledTimer() {
  let callback: (() => void) | undefined;
  let clearCount = 0;
  let delayMs: number | undefined;
  const handle = { unref: () => undefined } as unknown as ReturnType<typeof setTimeout>;
  return {
    get clearCount() {
      return clearCount;
    },
    get delayMs() {
      return delayMs;
    },
    setTimer: (nextCallback: () => void, ms: number): ReturnType<typeof setTimeout> => {
      callback = nextCallback;
      delayMs = ms;
      return handle;
    },
    clearTimer: (timer: ReturnType<typeof setTimeout>) => {
      expect(timer).toBe(handle);
      clearCount += 1;
    },
    fire() {
      if (callback === undefined) {
        throw new Error("timer was not scheduled");
      }
      callback();
    },
  };
}

class RotatingCredentialResolver implements GitHubMcpCredentialResolver {
  private resolveIndex = 0;
  private refreshIndex = 0;
  refreshes = 0;
  readonly refreshInputs: Array<Parameters<GitHubMcpCredentialResolver["refresh"]>[0]> = [];

  constructor(
    private readonly tokens: readonly string[],
    private readonly refreshTokens: readonly string[] = tokens,
  ) {}

  async resolve() {
    const token = this.tokens[Math.min(this.resolveIndex, this.tokens.length - 1)] ?? "token-a";
    this.resolveIndex += 1;
    return { ok: true as const, mode: "bearer" as const, token, tokenHash: token, vaultId: "vlt_1", credentialId: "cred_1" };
  }

  async refresh(input: Parameters<GitHubMcpCredentialResolver["refresh"]>[0]) {
    this.refreshes += 1;
    this.refreshInputs.push(input);
    const token = this.refreshTokens[Math.min(this.refreshIndex, this.refreshTokens.length - 1)] ?? "token-b";
    this.refreshIndex += 1;
    return { ok: true as const, mode: "bearer" as const, token, tokenHash: token, vaultId: "vlt_1", credentialId: "cred_1" };
  }
}

class RecordingSDKClient implements SDKClientLike {
	onerror: ((error: Error) => void) | undefined;
  connects = 0;
	closed = false;
	closeCount = 0;
  connectError: unknown;
  connectGate: Promise<void> | undefined;
  connectOptions: Array<{ readonly timeout?: number; readonly signal?: AbortSignal } | undefined> = [];
  listToolsError: unknown;
	callToolError: unknown;
	callToolGate: Promise<void> | undefined;
	listToolsGate: Promise<void> | undefined;
  callToolOptions: Array<{ readonly timeout?: number } | undefined> = [];
  listToolsOptions: Array<{ readonly timeout?: number } | undefined> = [];
  listToolsParams: unknown[] = [];
  callToolParams: Array<{ readonly name: string; readonly arguments?: Record<string, unknown> | undefined }> = [];
  tools: RecordingSDKTool[] = [
    { name: "create_issue", description: "Create an issue.", inputSchema: { type: "object" as const } },
  ];
  /** Cursor-keyed discovery pages; the "" key serves the first (cursorless) page. */
  pagesByCursor: Record<string, { readonly tools: RecordingSDKTool[]; readonly nextCursor?: string | undefined; readonly error?: unknown }> | undefined;

  async connect(_transport: unknown, options?: { readonly timeout?: number; readonly signal?: AbortSignal }) {
    this.connects += 1;
    this.connectOptions.push(options);
    if (this.connectGate !== undefined) {
      await this.connectGate;
    }
    if (this.connectError !== undefined) {
      throw this.connectError;
    }
  }

  async listTools(params?: unknown, options?: { readonly timeout?: number }) {
    this.listToolsParams.push(params);
    this.listToolsOptions.push(options);
		if (this.listToolsGate !== undefined) {
			await this.listToolsGate;
		}
    if (this.listToolsError !== undefined) {
      throw this.listToolsError;
    }
    if (this.pagesByCursor !== undefined) {
      const cursor = (params as { readonly cursor?: string | undefined } | undefined)?.cursor ?? "";
      const page = this.pagesByCursor[cursor];
      if (page === undefined) {
        throw new Error(`unexpected discovery cursor: ${cursor}`);
      }
      if (page.error !== undefined) {
        throw page.error;
      }
      return page.nextCursor === undefined ? { tools: page.tools } : { tools: page.tools, nextCursor: page.nextCursor };
    }
    return {
      tools: this.tools,
    };
  }

  async callTool(params: { readonly name: string; readonly arguments?: Record<string, unknown> | undefined }, _resultSchema?: unknown, options?: { readonly timeout?: number }) {
    this.callToolParams.push(params);
    this.callToolOptions.push(options);
		if (this.callToolGate !== undefined) {
			await this.callToolGate;
		}
    if (this.callToolError !== undefined) {
      throw this.callToolError;
    }
    return { content: [{ type: "text" as const, text: "ok" }] };
  }

  async close() {
		this.closeCount += 1;
    this.closed = true;
  }
}

function deferred<T>(): { readonly promise: Promise<T>; resolve(value: T): void } {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

async function until(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (predicate()) {
      return;
    }
    await Promise.resolve();
  }
  throw new Error("condition was not reached");
}
