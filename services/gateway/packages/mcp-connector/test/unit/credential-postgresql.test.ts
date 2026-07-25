import { expect, test } from "bun:test";
import { randomBytes } from "node:crypto";
import { SQLGitHubMcpCredentialResolver } from "../../src/credential.js";
import type { McpCredentialSQL } from "../../src/credential.js";

const databaseURL = process.env.TETRAL_TEST_DATABASE_URL;

test.skipIf(databaseURL === undefined)("resolves credentials through forced workspace RLS", async () => {
  const suffix = randomBytes(6).toString("hex");
  const role = `mcp_rls_${suffix}`;
  const schema = `mcp_rls_${suffix}`;
  const password = `pw_${suffix}`;
  const admin = new Bun.SQL({ url: databaseURL!, max: 1 });
  let app: Bun.SQL | undefined;
  try {
    await admin.unsafe(`CREATE ROLE ${role} LOGIN PASSWORD '${password}'`);
    await admin.unsafe(`CREATE SCHEMA ${schema} AUTHORIZATION ${role}`);
    await admin.unsafe(`SET search_path TO ${schema}`);
    await admin.unsafe(`CREATE TABLE sessions (workspace_id text NOT NULL, id text NOT NULL, vault_ids_json text NOT NULL)`);
    await admin.unsafe(`CREATE TABLE credentials (
      workspace_id text NOT NULL, id text NOT NULL, vault_id text NOT NULL,
      auth_public_json text NOT NULL, encrypted_auth bytea, archived_at text,
      revoked_at text, auth_type text NOT NULL
    )`);
    await admin.unsafe(`ALTER TABLE sessions OWNER TO ${role}`);
    await admin.unsafe(`ALTER TABLE credentials OWNER TO ${role}`);
    await admin`INSERT INTO sessions VALUES ('wksp_rls', 'sesn_rls', '["vlt_rls"]')`;
    const keyHex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const encrypted = await encryptAES256GCM(new TextEncoder().encode(JSON.stringify({
      type: "static_bearer",
      mcp_server_url: "https://api.githubcopilot.com/mcp/",
      token: "rls-token-sentinel",
    })), keyHex);
    await admin`INSERT INTO credentials VALUES (
      'wksp_rls', 'cred_rls', 'vlt_rls',
      ${JSON.stringify({ type: "static_bearer", mcp_server_url: "https://api.githubcopilot.com/mcp/" })},
      ${encrypted}, NULL, NULL, 'static_bearer'
    )`;
    for (const table of ["sessions", "credentials"]) {
      await admin.unsafe(`ALTER TABLE ${table} ENABLE ROW LEVEL SECURITY`);
      await admin.unsafe(`ALTER TABLE ${table} FORCE ROW LEVEL SECURITY`);
      await admin.unsafe(`CREATE POLICY workspace_scope ON ${table}
        USING (workspace_id = current_setting('tetral.workspace_id', true))`);
    }

    const appURL = new URL(databaseURL!);
    appURL.username = role;
    appURL.password = password;
    app = new Bun.SQL({ url: appURL.toString(), max: 1 });
    await app.unsafe(`SET search_path TO ${schema}`);
    const resolver = new SQLGitHubMcpCredentialResolver(app as unknown as McpCredentialSQL, keyHex);

    const resolved = await resolver.resolve({
      workspaceId: "wksp_rls",
      sessionId: "sesn_rls",
      mcpServerName: "github",
    });
    expect(resolved).toMatchObject({ ok: true, mode: "bearer", token: "rls-token-sentinel" });
    const wrongWorkspace = await resolver.resolve({
      workspaceId: "wksp_other",
      sessionId: "sesn_rls",
      mcpServerName: "github",
    });
    expect(wrongWorkspace).toEqual({ ok: false, error: "credential_required" });
  } finally {
    if (app !== undefined) {
      await app.close();
    }
    await admin.unsafe("SET search_path TO public");
    await admin.unsafe(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
    await admin.unsafe(`DROP ROLE IF EXISTS ${role}`);
    await admin.close();
  }
});

async function encryptAES256GCM(plaintext: Uint8Array, keyHex: string): Promise<Uint8Array> {
  const key = await crypto.subtle.importKey("raw", arrayBuffer(Uint8Array.fromHex(keyHex)), { name: "AES-GCM" }, false, ["encrypt"]);
  const nonce = crypto.getRandomValues(new Uint8Array(12));
  const ciphertext = new Uint8Array(await crypto.subtle.encrypt({ name: "AES-GCM", iv: arrayBuffer(nonce), tagLength: 128 }, key, arrayBuffer(plaintext)));
  const encoded = new Uint8Array(nonce.length + ciphertext.length);
  encoded.set(nonce, 0);
  encoded.set(ciphertext, nonce.length);
  return encoded;
}

function arrayBuffer(bytes: Uint8Array): ArrayBuffer {
  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer;
}
