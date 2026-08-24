import { expect, test } from "bun:test";
import { randomBytes } from "node:crypto";
import { McpSDKClient } from "../../src/client.js";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import { SQLGitHubMcpCredentialResolver } from "../../src/credential.js";
import type { McpCredentialSQL } from "../../src/credential.js";
import type { McpOAuthRefreshCompletedEvent } from "../../src/credential-update-path.js";
import { recordMcpOAuthRefreshCompleted } from "../../src/logger.js";
import { McpConnectorMetricsRegistry } from "../../src/metrics.js";

const databaseURL = process.env.TETRAL_TEST_DATABASE_URL;

test("concurrent SDK calls share one durable OAuth rotation and terminal failure preserves it", async () => {
  if (databaseURL === undefined) {
    throw new Error("TETRAL_TEST_DATABASE_URL is required for the durable OAuth concurrency proof");
  }
  const suffix = randomBytes(6).toString("hex");
  const role = `mcp_refresh_${suffix}`;
  const schema = `mcp_refresh_${suffix}`;
  const password = `pw_${suffix}`;
  const admin = new Bun.SQL({ url: databaseURL!, max: 1 });
  let app: Bun.SQL | undefined;
  const keyHex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
  const now = new Date("2026-08-10T12:00:00.000Z");
  const initialAuth = {
    type: "mcp_oauth", mcp_server_url: "https://api.githubcopilot.com/mcp/",
    access_token: "ACCESS_TOKEN_CANARY_OLD", expires_at: "2026-08-10T11:59:00.000Z",
    refresh: { refresh_token: "REFRESH_TOKEN_CANARY_OLD", client_id: "github-client",
      token_endpoint: "https://github.example.invalid/oauth/token", token_endpoint_auth: { type: "none" } },
  };
  const initialEncrypted = await encryptAES256GCM(new TextEncoder().encode(JSON.stringify(initialAuth)), keyHex);
  let issuerRequests = 0;
  let issuerFailure = false;
  const events: McpOAuthRefreshCompletedEvent[] = [];
  const metrics = new McpConnectorMetricsRegistry();
  const fetchFn = async (_input: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]): ReturnType<typeof fetch> => {
    issuerRequests += 1;
    expect(new Headers(init?.headers).get("accept")).toBe("application/json");
    if (issuerFailure) return new Response(JSON.stringify({ error: "RAW_ISSUER_ERROR_CANARY" }), { status: 503 });
    return Response.json({ access_token: "ACCESS_TOKEN_CANARY_ROTATED", refresh_token: "REFRESH_TOKEN_CANARY_ROTATED", expires_in: 3600 });
  };
  try {
    await admin.unsafe(`CREATE ROLE ${role} LOGIN PASSWORD '${password}'`);
    await admin.unsafe(`CREATE SCHEMA ${schema} AUTHORIZATION ${role}`);
    await admin.unsafe(`ALTER ROLE ${role} SET search_path TO ${schema}`);
    await admin.unsafe(`SET search_path TO ${schema}`);
    await admin.unsafe(`CREATE TABLE sessions (workspace_id text NOT NULL, id text NOT NULL, vault_ids_json text NOT NULL)`);
    await admin.unsafe(`CREATE TABLE credentials (
      workspace_id text NOT NULL, id text NOT NULL, vault_id text NOT NULL,
      auth_public_json text NOT NULL, encrypted_auth bytea, archived_at text, revoked_at text,
      auth_type text NOT NULL, mcp_server_url text NOT NULL, expires_at text, updated_at text
    )`);
    await admin.unsafe(`ALTER TABLE sessions OWNER TO ${role}`);
    await admin.unsafe(`ALTER TABLE credentials OWNER TO ${role}`);
    await admin`INSERT INTO sessions VALUES ('wksp_refresh', 'sesn_refresh', '["vlt_refresh"]')`;
    const publicAuth = JSON.stringify({ type: "mcp_oauth", mcp_server_url: initialAuth.mcp_server_url,
      expires_at: initialAuth.expires_at, refresh: { client_id: initialAuth.refresh.client_id,
        token_endpoint: initialAuth.refresh.token_endpoint, token_endpoint_auth: { type: "none" } } });
    await admin`INSERT INTO credentials VALUES (
      'wksp_refresh', 'cred_refresh', 'vlt_refresh', ${publicAuth}, ${initialEncrypted}, NULL, NULL,
      'mcp_oauth', ${initialAuth.mcp_server_url}, ${initialAuth.expires_at}, ${now.toISOString()}
    )`;
    for (const table of ["sessions", "credentials"]) {
      await admin.unsafe(`ALTER TABLE ${table} ENABLE ROW LEVEL SECURITY`);
      await admin.unsafe(`ALTER TABLE ${table} FORCE ROW LEVEL SECURITY`);
      await admin.unsafe(`CREATE POLICY workspace_scope ON ${table}
        USING (workspace_id = current_setting('tetral.workspace_id', true))
        WITH CHECK (workspace_id = current_setting('tetral.workspace_id', true))`);
    }
    const appURL = new URL(databaseURL!);
    appURL.username = role;
    appURL.password = password;
    app = new Bun.SQL({ url: appURL.toString(), max: 4 });
    const resolver = new SQLGitHubMcpCredentialResolver(
      app as unknown as McpCredentialSQL, keyHex, () => now, fetchFn, undefined, undefined,
      (event) => {
        events.push(event);
        recordMcpOAuthRefreshCompleted(undefined, (outcome) => metrics.recordRefreshAttempt(outcome), event);
      },
    );
    const transportTokens: string[] = [];
    const client = mcpClient(resolver, (token) => transportTokens.push(token));
    const results = await Promise.all([call(client, "thrd_a"), call(client, "thrd_b")]);
    expect(issuerRequests).toBe(1);
    expect(results.every((result) => result.content?.[0]?.type === "text")).toBe(true);
    expect(results.filter((result) => result.refreshTriggered === true)).toHaveLength(1);
    expect(transportTokens).toEqual(["ACCESS_TOKEN_CANARY_ROTATED"]);
    expect(client.connectionCount()).toBe(1);
    expect(events.map((event) => event.outcome).sort()).toEqual(["concurrent_winner_reused", "refreshed"]);
    expect(events.find((event) => event.outcome === "refreshed")).toMatchObject({
      durableWrite: "committed", refreshAttemptMetric: "success", httpStatusClass: "2xx",
    });
    expect(JSON.stringify(events)).not.toContain("TOKEN_CANARY");
    expect(metrics.render()).toContain('mcpconnector_refresh_attempts_total{outcome="success"} 1');
    expect(metrics.render()).toContain('mcpconnector_refresh_attempts_total{outcome="failed"} 0');
    const rotatedRows = await admin<{ auth_public_json: string; encrypted_auth: Uint8Array; expires_at: string }[]>`
      SELECT auth_public_json, encrypted_auth, expires_at FROM credentials WHERE id = 'cred_refresh'`;
    const rotated = JSON.parse(new TextDecoder().decode(
      await decryptAES256GCM(rotatedRows[0]!.encrypted_auth, keyHex),
    )) as { readonly access_token?: string; readonly expires_at?: string; readonly refresh?: { readonly refresh_token?: string } };
    expect(rotated).toMatchObject({
      access_token: "ACCESS_TOKEN_CANARY_ROTATED",
      expires_at: "2026-08-10T13:00:00.000Z",
      refresh: { refresh_token: "REFRESH_TOKEN_CANARY_ROTATED" },
    });
    expect(rotatedRows[0]!.expires_at).toBe("2026-08-10T13:00:00.000Z");
    expect(rotatedRows[0]!.auth_public_json).not.toContain("ACCESS_TOKEN_CANARY_ROTATED");
    expect(rotatedRows[0]!.auth_public_json).not.toContain("REFRESH_TOKEN_CANARY_ROTATED");
    await client.closeAll();

    await admin`UPDATE credentials SET auth_public_json = ${publicAuth}, encrypted_auth = ${initialEncrypted},
      expires_at = ${initialAuth.expires_at} WHERE workspace_id = 'wksp_refresh' AND id = 'cred_refresh'`;
    issuerFailure = true;
    issuerRequests = 0;
    events.length = 0;
    const failingClient = mcpClient(resolver, () => undefined);
    await expect(call(failingClient, "thrd_failure")).rejects.toThrow("credential refresh is temporarily unavailable");
    expect(issuerRequests).toBe(1);
    expect(events).toEqual([expect.objectContaining({ outcome: "failed", failureKind: "http_status",
      httpStatusClass: "5xx", durableWrite: "not_attempted", refreshAttemptMetric: "failed" })]);
    expect(JSON.stringify(events)).not.toContain("RAW_ISSUER_ERROR_CANARY");
    expect(metrics.render()).toContain('mcpconnector_refresh_attempts_total{outcome="failed"} 1');
    const rows = await admin<{ encrypted_auth: Uint8Array }[]>`SELECT encrypted_auth FROM credentials WHERE id = 'cred_refresh'`;
    expect(Buffer.from(rows[0]!.encrypted_auth).equals(Buffer.from(initialEncrypted))).toBe(true);
  } finally {
    if (app !== undefined) await app.close();
    await admin.unsafe("SET search_path TO public");
    await admin.unsafe(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
    await admin.unsafe(`DROP ROLE IF EXISTS ${role}`);
    await admin.close();
  }
});

function mcpClient(resolver: SQLGitHubMcpCredentialResolver, observeToken: (token: string) => void): McpSDKClient {
  return new McpSDKClient({
    credentialResolver: resolver, onToolsListChanged: async () => undefined,
    createTransport: ({ token }) => {
      observeToken(token ?? "");
      return new SuccessfulSDKTransport();
    },
  });
}

async function call(client: McpSDKClient, sessionThreadId: string) {
  return await client.callTool({ workspaceId: "wksp_refresh", sessionId: "sesn_refresh", sessionThreadId,
    mcpServerName: "github", toolName: "search", input: { query: "tetral" } });
}

class SuccessfulSDKTransport implements Transport {
  onclose?: () => void;
  onerror?: (error: Error) => void;
  onmessage: NonNullable<Transport["onmessage"]> = () => undefined;

  async start(): Promise<void> {}

  async send(message: Parameters<Transport["send"]>[0]): Promise<void> {
    if (!("id" in message) || !("method" in message)) return;
    let result: unknown;
    switch (message.method) {
      case "initialize":
        result = { protocolVersion: "2025-06-18", capabilities: { tools: {} }, serverInfo: { name: "oauth-proof", version: "1.0.0" } };
        break;
      case "tools/list":
        result = { tools: [] };
        break;
      case "tools/call":
        result = { content: [{ type: "text", text: "ok" }] };
        break;
      default:
        throw new Error(`unexpected MCP request ${message.method}`);
    }
    queueMicrotask(() => this.onmessage?.({ jsonrpc: "2.0", id: message.id, result } as Parameters<NonNullable<Transport["onmessage"]>>[0]));
  }

  async close(): Promise<void> {
    this.onclose?.();
  }
}

async function encryptAES256GCM(plaintext: Uint8Array, keyHex: string): Promise<Uint8Array> {
  const key = await crypto.subtle.importKey("raw", arrayBuffer(Uint8Array.fromHex(keyHex)), { name: "AES-GCM" }, false, ["encrypt"]);
  const nonce = crypto.getRandomValues(new Uint8Array(12));
  const ciphertext = new Uint8Array(await crypto.subtle.encrypt({ name: "AES-GCM", iv: arrayBuffer(nonce), tagLength: 128 }, key, arrayBuffer(plaintext)));
  const encoded = new Uint8Array(nonce.length + ciphertext.length);
  encoded.set(nonce, 0);
  encoded.set(ciphertext, nonce.length);
  return encoded;
}

async function decryptAES256GCM(ciphertext: Uint8Array, keyHex: string): Promise<Uint8Array> {
  const key = await crypto.subtle.importKey("raw", arrayBuffer(Uint8Array.fromHex(keyHex)), { name: "AES-GCM" }, false, ["decrypt"]);
  const plaintext = await crypto.subtle.decrypt(
    { name: "AES-GCM", iv: arrayBuffer(ciphertext.slice(0, 12)), tagLength: 128 },
    key,
    arrayBuffer(ciphertext.slice(12)),
  );
  return new Uint8Array(plaintext);
}

function arrayBuffer(bytes: Uint8Array): ArrayBuffer {
  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer;
}
