import { describe, expect, test } from "bun:test";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { ErrorCode } from "@modelcontextprotocol/sdk/types.js";
import {
  MCP_CONNECT_TIMEOUT_MS,
  MCP_CREDENTIAL_RESOLUTION_TIMEOUT_MS,
  MCP_RECONNECT_DELAYS_MS,
  MCP_RECONNECT_MAX_RETRIES,
  McpSDKClient,
  mcpToolsListChangedFailureLogRecord,
  streamableHTTPTransportOptions,
} from "../../src/client.js";
import type { SDKClientLike } from "../../src/client.js";
import type { GitHubMcpCredentialResolver } from "../../src/credential.js";

type RecordingSDKTool = Awaited<ReturnType<SDKClientLike["listTools"]>>["tools"][number] & {
  readonly enabled?: boolean | undefined;
};

describe("McpSDKClient", () => {
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
    expect(transports).toEqual([{ url: new URL("https://api.githubcopilot.com/mcp/"), token: "token-a" }]);
    expect(clients).toHaveLength(1);
    expect(clients[0]?.connects).toBe(1);
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
    const refreshAttempts: string[] = [];
    const client = new McpSDKClient({
      credentialResolver: credentials,
      onToolsListChanged: async () => undefined,
      onRefreshAttempt: (outcome) => {
        refreshAttempts.push(outcome);
      },
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
    expect(refreshAttempts).toEqual(["success"]);
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
    const refreshAttempts: string[] = [];
    const client = new McpSDKClient({
      credentialResolver: {
        resolve: async () => ({ ok: true, mode: "bearer", token: "token-b", tokenHash: "token-b", vaultId: "vlt_1", credentialId: "cred_1", refreshTriggered: true }),
        refresh: async () => ({ ok: false, error: "refresh_failed" }),
      },
      onToolsListChanged: async () => undefined,
      onRefreshAttempt: (outcome) => {
        refreshAttempts.push(outcome);
      },
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
    expect(refreshAttempts).toEqual(["success"]);
  });

  test("does not count locked-row credential reuse as a refresh attempt", async () => {
    const refreshAttempts: string[] = [];
    const client = new McpSDKClient({
      credentialResolver: {
        resolve: async () => ({ ok: true, mode: "bearer", token: "already-rotated", tokenHash: "already-rotated", vaultId: "vlt_1", credentialId: "cred_1" }),
        refresh: async () => ({ ok: false, error: "refresh_failed" }),
      },
      onToolsListChanged: async () => undefined,
      onRefreshAttempt: (outcome) => {
        refreshAttempts.push(outcome);
      },
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
    expect(refreshAttempts).toEqual([]);
  });

  test("records failed proactive refresh attempts during initial connection", async () => {
    const refreshAttempts: string[] = [];
    const client = new McpSDKClient({
      credentialResolver: {
        resolve: async () => ({ ok: false, error: "refresh_failed" }),
        refresh: async () => ({ ok: false, error: "refresh_failed" }),
      },
      onToolsListChanged: async () => undefined,
      onRefreshAttempt: (outcome) => {
        refreshAttempts.push(outcome);
      },
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
    expect(refreshAttempts).toEqual(["failed"]);
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
    const refreshAttempts: string[] = [];
    const client = new McpSDKClient({
      credentialResolver: credentials,
      onToolsListChanged: async () => undefined,
      onRefreshAttempt: (outcome) => {
        refreshAttempts.push(outcome);
      },
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
    expect(refreshAttempts).toEqual(["success"]);
  });

  test("records failed refresh attempts when credential refresh cannot mint a replacement", async () => {
    const refreshAttempts: string[] = [];
    const client = new McpSDKClient({
      credentialResolver: {
        resolve: async () => ({ ok: true, mode: "bearer", token: "token-a", tokenHash: "token-a", vaultId: "vlt_1", credentialId: "cred_1" }),
        refresh: async () => ({ ok: false, error: "refresh_failed" }),
      },
      onToolsListChanged: async () => undefined,
      onRefreshAttempt: (outcome) => {
        refreshAttempts.push(outcome);
      },
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
    expect(refreshAttempts).toEqual(["failed"]);
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
  tools: RecordingSDKTool[] = [
    { name: "create_issue", description: "Create an issue.", inputSchema: { type: "object" as const } },
  ];

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

  async listTools(_params?: unknown, options?: { readonly timeout?: number }) {
    this.listToolsOptions.push(options);
		if (this.listToolsGate !== undefined) {
			await this.listToolsGate;
		}
    if (this.listToolsError !== undefined) {
      throw this.listToolsError;
    }
    return {
      tools: this.tools,
    };
  }

  async callTool(_params: { readonly name: string; readonly arguments?: Record<string, unknown> | undefined }, _resultSchema?: unknown, options?: { readonly timeout?: number }) {
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
