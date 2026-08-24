/**
 * @packageDocumentation
 *
 * Resolves the GitHub MCP bearer credential visible through a session's vault
 * set. The resolver scopes reads to the workspace, admits only live OAuth or
 * static bearer rows whose public MCP URL matches the curated server, and
 * fails closed unless exactly one row matches before decrypting its payload.
 * The MCP SDK client calls this module for initial connection material and
 * authentication-failure refreshes; the module calls catalog lookup,
 * transactional SQL, Web Crypto, and the locked credential update path.
 * Plaintext tokens leave only in successful in-process resolution objects.
 * Selection, decryption, expiry, and locked-refresh failures use bounded error
 * discriminants; uncaught selection-query failures reject for the service
 * boundary to normalize.
 */

import { createHash } from "node:crypto";
import { catalogEntryByName, normalizeCatalogURL } from "./catalog.js";
export { REFRESH_SKEW_SECONDS } from "./credential-constants.js";
import { REFRESH_SKEW_SECONDS } from "./credential-constants.js";
import { SQLVaultGitHubMcpCredentialUpdatePath } from "./credential-update-path.js";
import type { GitHubMcpCredentialRefreshWriter } from "./credential-update-path.js";
import type { McpOAuthRefreshCompletedEvent } from "./credential-update-path.js";

type FetchLike = (input: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]) => ReturnType<typeof fetch>;

/**
 * Tagged-query and transaction surface required by credential resolution and
 * refresh. Resolution uses a transaction to establish workspace row-level
 * security before reading session and credential rows.
 */
export interface McpCredentialSQL {
  <T = unknown>(strings: TemplateStringsArray, ...values: unknown[]): PromiseLike<T>;
  begin?<T>(fn: (sql: McpCredentialSQL) => Promise<T>): Promise<T>;
}

/**
 * Supplies per-session GitHub bearer material to the MCP SDK client.
 * Implementations resolve against durable session-visible credentials on each
 * connection attempt and support one forced refresh after authentication
 * rejection. Successful plaintext material remains an in-process value.
 */
export interface GitHubMcpCredentialResolver {
  /**
   * Selects and resolves exactly one live credential for the workspace,
   * session, and curated MCP server identity.
   */
  resolve(input: {
    readonly workspaceId: string;
    readonly sessionId: string;
    readonly mcpServerName: string;
    readonly signal?: AbortSignal | undefined;
  }): Promise<GitHubMcpCredentialResolution>;
  /**
   * Re-resolves the selected row and delegates OAuth write-back when needed.
   * The previous token hash allows a concurrent refresh loser to reuse newly
   * rotated material, and `force` bypasses the proactive expiry window.
   */
  refresh(input: {
    readonly workspaceId: string;
    readonly sessionId: string;
    readonly mcpServerName: string;
    readonly vaultId: string;
    readonly credentialId: string;
    readonly previousTokenHash: string;
    readonly force: boolean;
    readonly signal?: AbortSignal | undefined;
  }): Promise<GitHubMcpCredentialResolution>;
}

/**
 * Fail-closed result of resolving GitHub MCP authentication material. Success
 * carries the bearer token for transport injection, its hash for connection
 * identity, and whether this call performed a durable refresh.
 */
export type GitHubMcpCredentialResolution =
  | { readonly ok: true; readonly mode: "bearer"; readonly token: string; readonly tokenHash: string; readonly vaultId: string; readonly credentialId: string; readonly refreshTriggered?: boolean | undefined }
  | { readonly ok: false; readonly error: GitHubMcpCredentialError };

/**
 * Bounded categories for selection, decryption, expiry, and locked-refresh
 * failures. Uncaught selection-query failures are outside this union.
 */
export type GitHubMcpCredentialError =
  | "credential_required"
  | "ambiguous"
  | "undecryptable"
  | "expired"
  | "refresh_unavailable"
  | "refresh_failed";

interface GitHubCredentialSQLRow {
  readonly id: string;
  readonly vault_id: string;
  readonly auth_public_json: string | Record<string, unknown>;
  readonly encrypted_auth?: unknown;
}

interface GitHubCredentialMatchedRow extends GitHubCredentialSQLRow {
  readonly mcp_server_url: string;
}

interface CredentialAuth {
  readonly type?: string | undefined;
  readonly mcp_server_url?: string | undefined;
  readonly token?: string | undefined;
  readonly access_token?: string | undefined;
  readonly expires_at?: string | undefined;
  readonly refresh?: OAuthRefresh | undefined;
}

interface OAuthRefresh {
  readonly refresh_token?: string | undefined;
  readonly client_id?: string | undefined;
  readonly token_endpoint?: string | undefined;
  readonly scope?: string | undefined;
  readonly resource?: string | undefined;
  readonly token_endpoint_auth?: OAuthTokenEndpointAuth | undefined;
}

interface OAuthTokenEndpointAuth {
  readonly type?: "none" | "client_secret_basic" | "client_secret_post" | string | undefined;
  readonly client_secret?: string | undefined;
}

/**
 * PostgreSQL-backed resolver for session-scoped GitHub MCP credentials. It
 * separates read-only selection and decryption from the injected update owner,
 * which serializes OAuth refresh and performs the credential-row write.
 */
export class SQLGitHubMcpCredentialResolver implements GitHubMcpCredentialResolver {
  private readonly refreshWriter: GitHubMcpCredentialRefreshWriter;

  constructor(
    private readonly sql: McpCredentialSQL,
    private readonly masterKeyHex: string,
    private readonly now: () => Date = () => new Date(),
    fetchFn: FetchLike = fetch,
    refreshWriter?: GitHubMcpCredentialRefreshWriter,
    refreshHTTPTimeoutMs?: number,
    onRefreshCompleted?: ((event: McpOAuthRefreshCompletedEvent) => void) | undefined,
  ) {
    this.refreshWriter = refreshWriter ?? new SQLVaultGitHubMcpCredentialUpdatePath(sql, masterKeyHex, now, fetchFn, refreshHTTPTimeoutMs, onRefreshCompleted);
  }

  /**
   * Resolves one matching credential, refreshing a near-expiry OAuth row when
   * required and returning no material when selection or decryption fails.
   */
  async resolve(input: {
    readonly workspaceId: string;
    readonly sessionId: string;
    readonly mcpServerName: string;
    readonly signal?: AbortSignal | undefined;
  }): Promise<GitHubMcpCredentialResolution> {
    const catalog = catalogEntryByName(input.mcpServerName);
    if (catalog === undefined) {
      return fail("credential_required");
    }
    const rows = await this.loadMatchingRows(input);
    if (rows.length === 0) {
      return fail("credential_required");
    }
    if (rows.length > 1) {
      return fail("ambiguous");
    }
    return await this.resolveMatchedCredential(input, rows[0], { force: false, signal: input.signal });
  }

  /**
   * Selects the credential again and asks the update owner to refresh it when
   * forced or near expiry. Row locking in the update owner serializes refresh
   * requests and lets waiters reuse detected winner material.
   */
  async refresh(input: {
    readonly workspaceId: string;
    readonly sessionId: string;
    readonly mcpServerName: string;
    readonly vaultId: string;
    readonly credentialId: string;
    readonly previousTokenHash: string;
    readonly force: boolean;
    readonly signal?: AbortSignal | undefined;
  }): Promise<GitHubMcpCredentialResolution> {
    const rows = await this.loadMatchingRows(input);
    const row = rows.find((candidate) => candidate.vault_id === input.vaultId && candidate.id === input.credentialId);
    if (row === undefined) {
      return fail("credential_required");
    }
    return await this.resolveMatchedCredential(input, row, {
      force: input.force,
      previousTokenHash: input.previousTokenHash,
      signal: input.signal,
    });
  }

  private async loadMatchingRows(input: {
    readonly workspaceId: string;
    readonly sessionId: string;
    readonly mcpServerName: string;
  }): Promise<readonly GitHubCredentialMatchedRow[]> {
    const catalog = catalogEntryByName(input.mcpServerName);
    if (catalog === undefined) {
      return [];
    }
    if (this.sql.begin === undefined) {
      return [];
    }
    const rows = await this.sql.begin(async (tx) => {
      await setWorkspaceRLS(tx, input.workspaceId);
      return await tx<readonly GitHubCredentialSQLRow[]>`
        WITH session_vaults AS (
          SELECT jsonb_array_elements_text(vault_ids_json::jsonb) AS vault_id
            FROM sessions
           WHERE workspace_id = ${input.workspaceId}
             AND id = ${input.sessionId}
        )
        SELECT c.id, c.vault_id, c.auth_public_json, c.encrypted_auth
          FROM credentials c
          JOIN session_vaults sv ON sv.vault_id = c.vault_id
         WHERE c.workspace_id = ${input.workspaceId}
           AND c.archived_at IS NULL
           AND c.revoked_at IS NULL
           AND c.auth_type IN ('mcp_oauth', 'static_bearer')
         ORDER BY c.vault_id ASC, c.id ASC
      `;
    });
    const matched: GitHubCredentialMatchedRow[] = [];
    for (const row of rows) {
      const mcpServerURL = mcpServerURLFromPublicAuth(row.auth_public_json);
      if (mcpServerURL !== undefined && catalogURLMatches(mcpServerURL, catalog.url)) {
        matched.push({ ...row, mcp_server_url: mcpServerURL });
      }
    }
    return matched;
  }

  private async resolveMatchedCredential(input: {
    readonly workspaceId: string;
    readonly sessionId: string;
    readonly mcpServerName: string;
  }, row: GitHubCredentialMatchedRow | undefined, options: {
    readonly force: boolean;
    readonly previousTokenHash?: string | undefined;
    readonly signal?: AbortSignal | undefined;
  }): Promise<GitHubMcpCredentialResolution> {
    const catalog = catalogEntryByName(input.mcpServerName);
    if (catalog === undefined) {
      return fail("credential_required");
    }
    if (row === undefined || row.encrypted_auth === undefined || row.encrypted_auth === null) {
      return fail("undecryptable");
    }
    let auth: CredentialAuth;
    try {
      const plaintext = await decryptAES256GCM(databaseBytes(row.encrypted_auth), this.masterKeyHex);
      auth = JSON.parse(new TextDecoder().decode(plaintext)) as CredentialAuth;
    } catch {
      return fail("undecryptable");
    }
    if (!catalogURLMatches(auth.mcp_server_url ?? "", catalog.url)) {
      return fail("undecryptable");
    }
    if (auth.type === "static_bearer") {
      if (options.force) {
        return fail("credential_required");
      }
      return nonEmpty(auth.token) ? useToken(auth.token, row) : fail("undecryptable");
    }
    if (auth.type !== "mcp_oauth" || !nonEmpty(auth.access_token)) {
      return fail("undecryptable");
    }
    const action = oauthRefreshAction(auth, this.now(), options.force);
    if (action === "expired") {
      return fail("expired");
    }
    if (action === "refresh") {
      const refreshed = await this.refreshWriter.refreshOAuthCredential({
        workspaceId: input.workspaceId,
        sessionId: input.sessionId,
        mcpServerName: input.mcpServerName,
        row,
        vaultId: row.vault_id,
        credentialId: row.id,
        previousTokenHash: options.previousTokenHash,
        force: options.force,
        signal: options.signal,
      });
      return refreshed;
    }
    return useToken(auth.access_token, row);
  }
}

async function setWorkspaceRLS(sql: McpCredentialSQL, workspaceId: string): Promise<void> {
  await sql`SELECT set_config('tetral.workspace_id', ${workspaceId}, true)`;
}

function oauthRefreshAction(auth: CredentialAuth, now: Date, force: boolean): "use" | "expired" | "refresh" {
  if (force) {
    return auth.refresh === undefined ? "expired" : "refresh";
  }
  if (!nonEmpty(auth.expires_at)) {
    return "use";
  }
  const expiresAt = Date.parse(auth.expires_at);
  if (!Number.isFinite(expiresAt)) {
    return "expired";
  }
  if (auth.refresh !== undefined && expiresAt <= now.getTime() + REFRESH_SKEW_SECONDS * 1000) {
    return "refresh";
  }
  if (expiresAt <= now.getTime()) {
    return "expired";
  }
  return "use";
}

function fail(error: GitHubMcpCredentialError): GitHubMcpCredentialResolution {
  return { ok: false, error };
}

function useToken(token: string, row: Pick<GitHubCredentialSQLRow, "id" | "vault_id">): GitHubMcpCredentialResolution {
  return {
    ok: true,
    mode: "bearer",
    token,
    tokenHash: createHash("sha256").update(token).digest("hex"),
    vaultId: row.vault_id,
    credentialId: row.id,
  };
}

function catalogURLMatches(candidate: string, catalog: string): boolean {
  try {
    return normalizeCatalogURL(candidate) === normalizeCatalogURL(catalog);
  } catch {
    return false;
  }
}

function mcpServerURLFromPublicAuth(value: string | Record<string, unknown>): string | undefined {
  let parsed: unknown = value;
  if (typeof value === "string") {
    try {
      parsed = JSON.parse(value) as unknown;
    } catch {
      return undefined;
    }
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    return undefined;
  }
  const candidate = (parsed as { readonly mcp_server_url?: unknown }).mcp_server_url;
  return typeof candidate === "string" ? candidate : undefined;
}

function nonEmpty(value: string | undefined): value is string {
  return value !== undefined && value.length > 0;
}

async function decryptAES256GCM(ciphertext: Uint8Array, hexKey: string): Promise<Uint8Array> {
  const key = await importAESKey(hexKey);
  if (ciphertext.byteLength < 28) {
    throw new Error("ciphertext is too short");
  }
  const nonce = ciphertext.slice(0, 12);
  const sealed = ciphertext.slice(12);
  const plaintext = await crypto.subtle.decrypt({ name: "AES-GCM", iv: arrayBuffer(nonce), tagLength: 128 }, key, arrayBuffer(sealed));
  return new Uint8Array(plaintext);
}

async function importAESKey(hexKey: string): Promise<CryptoKey> {
  const keyBytes = bytesFromHex(hexKey);
  if (keyBytes.byteLength !== 32) {
    throw new Error("AES-256-GCM key must be 32 bytes");
  }
  return await crypto.subtle.importKey("raw", arrayBuffer(keyBytes), "AES-GCM", false, ["decrypt", "encrypt"]);
}

function bytesFromHex(value: string): Uint8Array {
  if (!/^(?:[0-9a-fA-F]{2})*$/.test(value)) {
    throw new Error("hex value is malformed");
  }
  const output = new Uint8Array(value.length / 2);
  for (let index = 0; index < output.length; index += 1) {
    output[index] = Number.parseInt(value.slice(index * 2, index * 2 + 2), 16);
  }
  return output;
}

function databaseBytes(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) {
    return value;
  }
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  throw new Error("database bytea value is not bytes");
}

function arrayBuffer(bytes: Uint8Array): ArrayBuffer {
  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer;
}
