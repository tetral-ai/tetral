import { readFile } from "node:fs/promises";
import { createRuntimeBindingTokenVerifier } from "@tetral/gateway-protocol/src/binding-token.js";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import { McpSDKClient } from "../../src/client.js";
import { SQLGitHubMcpCredentialResolver } from "../../src/credential.js";
import type { McpCredentialSQL } from "../../src/credential.js";
import type { McpOAuthRefreshCompletedEvent } from "../../src/credential-update-path.js";
import { InMemoryMcpIdempotencyStore } from "../../src/idempotency.js";
import { createMcpConnectorGrpcServer } from "../../src/server.js";
import { McpConnectorServiceShell } from "../../src/service.js";
import type { McpConnectorLogger } from "../../src/service.js";

const inputPath = process.argv[2];
if (inputPath === undefined) throw new Error("fixture input path is required");
const input = JSON.parse(await readFile(inputPath, "utf8")) as {
  readonly schema: string;
  readonly workspaceId: string;
  readonly sessionId: string;
};
if (input.schema !== "public") throw new Error("fixture schema is invalid");
const databaseURL = process.env.TETRAL_TEST_DATABASE_URL;
const runtimeDatabaseURL = process.env.TETRAL_TEST_RUNTIME_DATABASE_URL;
if (databaseURL === undefined) throw new Error("TETRAL_TEST_DATABASE_URL is required");
if (runtimeDatabaseURL === undefined) throw new Error("TETRAL_TEST_RUNTIME_DATABASE_URL is required");

const masterKeyHex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const vaultId = `vlt_${input.sessionId}`;
const credentialId = `cred_${input.sessionId}`;
const oldAccessToken = "OAUTH_ACCESS_TOKEN_OLD_CANARY";
const oldRefreshToken = "OAUTH_REFRESH_TOKEN_OLD_CANARY";
const rotatedAccessToken = "OAUTH_ACCESS_TOKEN_ROTATED_CANARY";
const rotatedRefreshToken = "OAUTH_REFRESH_TOKEN_ROTATED_CANARY";
const expiredAt = new Date(Date.now() - 60_000).toISOString();
const initialAuth = {
  type: "mcp_oauth",
  mcp_server_url: "https://api.githubcopilot.com/mcp/",
  access_token: oldAccessToken,
  expires_at: expiredAt,
  refresh: {
    refresh_token: oldRefreshToken,
    client_id: "github-client",
    token_endpoint: "https://github.example.invalid/oauth/token",
    token_endpoint_auth: { type: "none" },
  },
};
const encryptedAuth = await encryptAES256GCM(
  new TextEncoder().encode(JSON.stringify(initialAuth)),
  masterKeyHex,
);
const admin = new Bun.SQL({ url: databaseURL, max: 1 });
await admin.unsafe(`SET search_path TO ${input.schema}, pg_catalog`);
const now = new Date().toISOString();
await admin`INSERT INTO vaults (workspace_id, id, display_name, created_at, updated_at)
  VALUES (${input.workspaceId}, ${vaultId}, 'OAuth composition vault', ${now}, ${now})`;
await admin`UPDATE sessions SET vault_ids_json=${JSON.stringify([vaultId])}
  WHERE workspace_id=${input.workspaceId} AND id=${input.sessionId}`;
const publicAuth = JSON.stringify({
  type: "mcp_oauth",
  mcp_server_url: initialAuth.mcp_server_url,
  expires_at: initialAuth.expires_at,
  refresh: {
    client_id: initialAuth.refresh.client_id,
    token_endpoint: initialAuth.refresh.token_endpoint,
    token_endpoint_auth: { type: "none" },
  },
});
await admin`INSERT INTO credentials (
  workspace_id, id, vault_id, display_name, auth_type, auth_public_json,
  mcp_server_url, expires_at, encrypted_auth, created_at, updated_at
) VALUES (
  ${input.workspaceId}, ${credentialId}, ${vaultId}, 'GitHub OAuth', 'mcp_oauth',
  ${publicAuth}, ${initialAuth.mcp_server_url}, ${expiredAt}, ${encryptedAuth}, ${now}, ${now}
)`;

const runtime = new Bun.SQL({ url: runtimeDatabaseURL, max: 4 });
const scopedSQL = {
  begin: async <T>(fn: (sql: McpCredentialSQL) => Promise<T>): Promise<T> =>
    await runtime.begin(async (tx) => {
      await tx.unsafe(`SET LOCAL search_path TO ${input.schema}, pg_catalog`);
      return await fn(tx as unknown as McpCredentialSQL);
    }),
} as unknown as McpCredentialSQL;

let tokenEndpointAttempts = 0;
let toolsListCalls = 0;
const transportTokens: string[] = [];
const refreshEvents: McpOAuthRefreshCompletedEvent[] = [];
const records: unknown[] = [];
const logger: McpConnectorLogger = {
  info: (record) => records.push(record),
  error: (record) => records.push(record),
};
const resolver = new SQLGitHubMcpCredentialResolver(
  scopedSQL,
  masterKeyHex,
  () => new Date(),
  async () => {
    tokenEndpointAttempts += 1;
    return Response.json({
      access_token: rotatedAccessToken,
      refresh_token: rotatedRefreshToken,
      expires_in: 3600,
    });
  },
  undefined,
  undefined,
  (event) => refreshEvents.push(event),
);
const client = new McpSDKClient({
  credentialResolver: resolver,
  onToolsListChanged: async () => undefined,
  createTransport: ({ token }) => {
    transportTokens.push(token ?? "");
    return new ManifestSDKTransport(() => {
      toolsListCalls += 1;
    });
  },
});
const service = new McpConnectorServiceShell({
  ready: () => true,
  client,
  logger,
  authenticator: {
    authenticate: async () => ({
      ok: true,
      serviceAccount: { namespace: "tetral-system", name: "bridge", podUid: "bridge-pod" },
    }),
  },
  runtimeBindingTokenVerifier: createRuntimeBindingTokenVerifier({
    hmacKey: "oauth-composition-binding-key-32bytes",
  }),
  idempotencyStore: new InMemoryMcpIdempotencyStore({ mcpServerName: "github", toolName: "create_issue", inputJson: "{}" }),
  manifestChangeNotifier: {
    notify: async () => ({ ok: true, duplicate: false }),
  },
});
const grpcServer = createMcpConnectorGrpcServer(service);
const port = await grpcServer.bind("127.0.0.1:0");
process.stdout.write(`${JSON.stringify({ address: `127.0.0.1:${port}` })}\n`);

await new Promise<void>((resolve) => process.stdin.once("data", () => resolve()));
const rows = await admin<{ encrypted_auth: Uint8Array; expires_at: string }[]>`
  SELECT encrypted_auth, expires_at FROM credentials
  WHERE workspace_id=${input.workspaceId} AND vault_id=${vaultId} AND id=${credentialId}`;
const rotated = JSON.parse(new TextDecoder().decode(
  await decryptAES256GCM(rows[0]!.encrypted_auth, masterKeyHex),
)) as { readonly access_token?: string; readonly expires_at?: string; readonly refresh?: { readonly refresh_token?: string } };
const serializedRecords = JSON.stringify(records);
const finalLine = `${JSON.stringify({
  tokenEndpointAttempts,
  toolsListCalls,
  usedRotatedToken: transportTokens.length === 1 && transportTokens[0] === rotatedAccessToken,
  durableRotation: rotated.access_token === rotatedAccessToken &&
    rotated.refresh?.refresh_token === rotatedRefreshToken &&
    rotated.expires_at === rows[0]!.expires_at &&
    Date.parse(rows[0]!.expires_at) > Date.now(),
  refreshOutcomes: refreshEvents.map((event) => ({
    outcome: event.outcome,
    durableWrite: event.durableWrite,
    httpStatusClass: event.httpStatusClass,
  })),
  logsRedacted: !serializedRecords.includes(oldAccessToken) &&
    !serializedRecords.includes(oldRefreshToken) &&
    !serializedRecords.includes(rotatedAccessToken) &&
    !serializedRecords.includes(rotatedRefreshToken),
})}\n`;
await new Promise<void>((resolve, reject) => {
  process.stdout.write(finalLine, (error) => error === null ? resolve() : reject(error));
});
process.exit(0);

class ManifestSDKTransport implements Transport {
  onclose: () => void = () => undefined;
  onerror: (error: Error) => void = () => undefined;
  onmessage: NonNullable<Transport["onmessage"]> = () => undefined;

  constructor(private readonly recordList: () => void) {}

  async start(): Promise<void> {}

  async send(message: Parameters<Transport["send"]>[0]): Promise<void> {
    if (!("id" in message) || !("method" in message)) return;
    let result: unknown;
    switch (message.method) {
      case "initialize":
        result = {
          protocolVersion: "2025-06-18",
          capabilities: { tools: { listChanged: true } },
          serverInfo: { name: "oauth-manifest-proof", version: "1.0.0" },
        };
        break;
      case "tools/list":
        this.recordList();
        result = {
          tools: [{
            name: "github_search",
            description: "Search GitHub after OAuth refresh",
            inputSchema: { type: "object", properties: { query: { type: "string" } } },
          }],
        };
        break;
      default:
        throw new Error(`unexpected MCP request ${message.method}`);
    }
    queueMicrotask(() => this.onmessage({
      jsonrpc: "2.0",
      id: message.id,
      result,
    } as Parameters<NonNullable<Transport["onmessage"]>>[0]));
  }

  async close(): Promise<void> {
    this.onclose?.();
  }
}

async function encryptAES256GCM(plaintext: Uint8Array, keyHex: string): Promise<Uint8Array> {
  const key = await crypto.subtle.importKey("raw", arrayBuffer(Uint8Array.fromHex(keyHex)), { name: "AES-GCM" }, false, ["encrypt"]);
  const nonce = crypto.getRandomValues(new Uint8Array(12));
  const ciphertext = new Uint8Array(await crypto.subtle.encrypt(
    { name: "AES-GCM", iv: arrayBuffer(nonce), tagLength: 128 },
    key,
    arrayBuffer(plaintext),
  ));
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
