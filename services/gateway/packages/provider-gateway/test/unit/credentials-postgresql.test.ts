import { expect, test } from "bun:test";
import { randomBytes } from "node:crypto";
import { SQLGatewayCredentialStore } from "../../src/providers/credentials.js";
import type { GatewayCredentialSQL } from "../../src/providers/credentials.js";

const databaseURL = process.env.TETRAL_TEST_DATABASE_URL;

test.skipIf(databaseURL === undefined)("session credential lookup is scoped through forced workspace RLS", async () => {
  const suffix = randomBytes(6).toString("hex");
  const role = `gateway_rls_${suffix}`;
  const schema = `gateway_rls_${suffix}`;
  const password = `pw_${suffix}`;
  const admin = new Bun.SQL({ url: databaseURL!, max: 1 });
  let app: Bun.SQL | undefined;
  try {
    await admin.unsafe(`CREATE ROLE ${role} LOGIN PASSWORD '${password}'`);
    await admin.unsafe(`CREATE SCHEMA ${schema} AUTHORIZATION ${role}`);
    await admin.unsafe(`SET search_path TO ${schema}`);
    await admin.unsafe(`CREATE TABLE session_provider_auth (
      workspace_id text NOT NULL, session_id text NOT NULL, provider_id text NOT NULL,
      vault_id text NOT NULL, credential_id text NOT NULL, access_mode text NOT NULL,
      deleted_at text, updated_at text NOT NULL
    )`);
    await admin.unsafe(`CREATE TABLE credentials (
      workspace_id text NOT NULL, vault_id text NOT NULL, id text NOT NULL,
      auth_type text, provider_id text, access_mode text, encrypted_auth bytea,
      archived_at text, revoked_at text
    )`);
    for (const table of ["session_provider_auth", "credentials"]) {
      await admin.unsafe(`ALTER TABLE ${table} OWNER TO ${role}`);
    }
    await admin`INSERT INTO session_provider_auth VALUES (
      'wksp_rls', 'sesn_rls', 'anthropic', 'vlt_rls', 'cred_rls', 'user_api_key', NULL, '2026-07-11T00:00:00Z'
    )`;
    await admin`INSERT INTO credentials VALUES (
      'wksp_rls', 'vlt_rls', 'cred_rls', 'provider_api_key', 'anthropic', 'user_api_key', ${new Uint8Array([1, 2, 3])}, NULL, NULL
    )`;
    for (const table of ["session_provider_auth", "credentials"]) {
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
    const store = new SQLGatewayCredentialStore(app as unknown as GatewayCredentialSQL);

    await expect(store.loadActiveSessionProviderAuth({ workspaceId: "wksp_rls", sessionId: "sesn_rls" })).resolves.toMatchObject([
      { providerId: "anthropic", credentialId: "cred_rls", archived: false, revoked: false },
    ]);
    await expect(store.loadActiveSessionProviderAuth({ workspaceId: "wksp_other", sessionId: "sesn_rls" })).resolves.toEqual([]);
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
