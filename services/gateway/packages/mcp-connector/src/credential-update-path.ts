/**
 * @packageDocumentation
 *
 * Implements the connector adapter to the Vault-domain persistence path for
 * GitHub MCP OAuth refreshes. The credential resolver passes one selected
 * encrypted row into this module, which locks that live row by workspace,
 * vault, and credential identity across the refresh request and durable
 * rewrite. A waiter reuses the locked material when its token hash changes or
 * a known parseable expiry moves forward. The module calls transactional SQL,
 * Web Crypto, and the OAuth token endpoint; it writes only redacted public
 * metadata plus encrypted secret material and returns bounded resolution
 * outcomes to the resolver.
 */

import { createHash } from "node:crypto";
import { normalizeCatalogURL } from "./catalog.js";
import { REFRESH_SKEW_SECONDS } from "./credential-constants.js";
import { MCP_REFRESH_HTTP_TIMEOUT_MS } from "./phase-budgets.js";
import type {
  GitHubMcpCredentialResolution,
  McpCredentialSQL,
} from "./credential.js";

type FetchLike = (input: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]) => ReturnType<typeof fetch>;

/**
 * Persistence-capable refresh boundary used by the credential resolver.
 * Implementations either return current bearer material or refresh and persist
 * one selected credential without exposing refresh secrets to the caller.
 */
export interface GitHubMcpCredentialRefreshWriter {
  /**
   * Refreshes the selected OAuth row, or reuses material written by a
   * concurrent winner. The previous token hash lets the implementation detect
   * rotation without receiving another plaintext token from the caller.
   */
  refreshOAuthCredential(input: {
    readonly workspaceId: string;
    readonly row: GitHubCredentialUpdateRow;
    readonly vaultId: string;
    readonly credentialId: string;
    readonly previousTokenHash?: string | undefined;
    readonly force: boolean;
    readonly signal?: AbortSignal | undefined;
  }): Promise<GitHubMcpCredentialResolution>;
}

/**
 * Minimal selected credential row handed from resolution to the update owner.
 * Identity and public routing metadata remain separate from the opaque
 * encrypted authentication payload.
 */
export interface GitHubCredentialUpdateRow {
  readonly id: string;
  readonly vault_id: string;
  readonly mcp_server_url: string;
  readonly encrypted_auth?: unknown;
}

interface GitHubCredentialLockedRow {
  readonly id: string;
  readonly vault_id: string;
  readonly auth_public_json: string | Record<string, unknown>;
  readonly encrypted_auth?: unknown;
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

interface OAuthRefreshResult {
  readonly accessToken: string;
  readonly refreshToken: string;
  readonly expiresAt: string;
}

// Adapter for the Vault-domain locked credential update path. The credential
// lock spans the provider refresh call and the durable write.
/**
 * SQL and Web Crypto implementation of the single-flight OAuth update path.
 * This class issues the MCP connector's only credential-row update and keeps
 * the row lock held through token rotation, encryption, and write-back.
 */
export class SQLVaultGitHubMcpCredentialUpdatePath implements GitHubMcpCredentialRefreshWriter {
  constructor(
    private readonly sql: McpCredentialSQL,
    private readonly masterKeyHex: string,
    private readonly now: () => Date = () => new Date(),
    private readonly fetchFn: FetchLike = fetch,
    private readonly refreshHTTPTimeoutMs: number = MCP_REFRESH_HTTP_TIMEOUT_MS,
  ) {}

  /**
   * Locks and re-reads the selected live row before deciding whether to call
   * the token endpoint. A lock loser returns the winner's bearer material when
   * the token hash changes or a known parseable expiry moves forward; failed
   * refreshes leave the row unchanged and return a fail-closed resolution.
   */
  async refreshOAuthCredential(input: {
    readonly workspaceId: string;
    readonly row: GitHubCredentialUpdateRow;
    readonly vaultId: string;
    readonly credentialId: string;
    readonly previousTokenHash?: string | undefined;
    readonly force: boolean;
    readonly signal?: AbortSignal | undefined;
  }): Promise<GitHubMcpCredentialResolution> {
    if (this.sql.begin === undefined) {
      return fail("refresh_failed");
    }
    try {
      // Capture the caller's snapshot before waiting so expiry-only refresh wins remain observable.
      const previousExpiresAt = await this.decryptCredentialExpiry(input.row.encrypted_auth);
      return await this.sql.begin(async (tx) => {
        await tx`SELECT set_config('tetral.workspace_id', ${input.workspaceId}, true)`;
        const locked = await tx<readonly GitHubCredentialLockedRow[]>`
          SELECT id, vault_id, auth_public_json, encrypted_auth
            FROM credentials
           WHERE workspace_id = ${input.workspaceId}
             AND vault_id = ${input.vaultId}
             AND id = ${input.credentialId}
             AND archived_at IS NULL
             AND revoked_at IS NULL
           FOR UPDATE
        `;
        const lockedRow = locked[0];
        if (
          lockedRow === undefined
          || lockedRow.vault_id !== input.vaultId
          || lockedRow.id !== input.credentialId
        ) {
          return fail("credential_required");
        }
        if (lockedRow.encrypted_auth === undefined || lockedRow.encrypted_auth === null) {
          return fail("undecryptable");
        }
        const current = await this.decryptCredential(lockedRow);
        if (current === undefined || current.type !== "mcp_oauth" || !nonEmpty(current.access_token)) {
          return fail("undecryptable");
        }
        const publicMcpServerURL = mcpServerURLFromPublicAuth(lockedRow.auth_public_json);
        if (publicMcpServerURL === undefined || !catalogURLMatches(current.mcp_server_url ?? "", publicMcpServerURL)) {
          return fail("undecryptable");
        }
        const currentResolution = useToken(current.access_token, lockedRow);
        if (expiryMovedForward(previousExpiresAt, current.expires_at)) {
          return currentResolution;
        }
        if (
          input.previousTokenHash !== undefined &&
          currentResolution.ok &&
          currentResolution.mode === "bearer" &&
          currentResolution.tokenHash !== input.previousTokenHash
        ) {
          return currentResolution;
        }
        const currentAction = oauthRefreshAction(current, this.now(), false);
        if (!input.force && currentAction === "use") {
          return currentResolution;
        }
        if (current.refresh === undefined) {
          return fail(input.force ? "refresh_failed" : currentAction === "expired" ? "expired" : "refresh_failed");
        }
        const refreshed = await this.refreshGitHubOAuth(current.refresh, input.signal);
        if (refreshed === undefined) {
          return fail("refresh_failed");
        }
        const nextAuth = materializeRefreshedAuth(current, refreshed);
        const publicAuth = publicAuthFromSecret(nextAuth);
        const encrypted = await encryptAES256GCM(new TextEncoder().encode(JSON.stringify(nextAuth)), this.masterKeyHex);
        await tx`
          UPDATE credentials
             SET auth_public_json = ${JSON.stringify(publicAuth)},
                 mcp_server_url = ${publicAuth.mcp_server_url ?? ""},
                 expires_at = ${publicAuth.expires_at ?? ""},
                 encrypted_auth = ${encrypted},
                 updated_at = ${this.now().toISOString()}
           WHERE workspace_id = ${input.workspaceId}
             AND vault_id = ${input.row.vault_id}
             AND id = ${input.row.id}
        `;
        return { ...useToken(nextAuth.access_token ?? "", lockedRow), refreshTriggered: true };
      });
    } catch {
      return fail("refresh_failed");
    }
  }

  private async decryptCredentialExpiry(encryptedAuth: unknown): Promise<string | undefined> {
    if (encryptedAuth === undefined || encryptedAuth === null) {
      return undefined;
    }
    try {
      const plaintext = await decryptAES256GCM(databaseBytes(encryptedAuth), this.masterKeyHex);
      const auth = JSON.parse(new TextDecoder().decode(plaintext)) as CredentialAuth;
      return auth.expires_at;
    } catch {
      return undefined;
    }
  }

  private async decryptCredential(row: GitHubCredentialLockedRow): Promise<CredentialAuth | undefined> {
    if (row.encrypted_auth === undefined || row.encrypted_auth === null) {
      return undefined;
    }
    try {
      const plaintext = await decryptAES256GCM(databaseBytes(row.encrypted_auth), this.masterKeyHex);
      const auth = JSON.parse(new TextDecoder().decode(plaintext)) as CredentialAuth;
      const publicMcpServerURL = mcpServerURLFromPublicAuth(row.auth_public_json);
      if (publicMcpServerURL === undefined || !catalogURLMatches(auth.mcp_server_url ?? "", publicMcpServerURL)) {
        return undefined;
      }
      return auth;
    } catch {
      return undefined;
    }
  }

  private async refreshGitHubOAuth(refresh: OAuthRefresh, parentSignal?: AbortSignal): Promise<OAuthRefreshResult | undefined> {
    if (!nonEmpty(refresh.refresh_token) || !nonEmpty(refresh.client_id) || !nonEmpty(refresh.token_endpoint)) {
      return undefined;
    }
    const form = new URLSearchParams();
    form.set("grant_type", "refresh_token");
    form.set("refresh_token", refresh.refresh_token);
    if (nonEmpty(refresh.scope)) {
      form.set("scope", refresh.scope);
    }
    if (nonEmpty(refresh.resource)) {
      form.set("resource", refresh.resource);
    }
    const endpointAuth = refresh.token_endpoint_auth ?? { type: "none" };
    const headers = new Headers({ "Content-Type": "application/x-www-form-urlencoded" });
    if (endpointAuth.type === "client_secret_basic") {
      if (!nonEmpty(endpointAuth.client_secret)) {
        return undefined;
      }
      headers.set("Authorization", `Basic ${btoa(`${refresh.client_id}:${endpointAuth.client_secret}`)}`);
    } else {
      if (endpointAuth.type !== undefined && endpointAuth.type !== "none" && endpointAuth.type !== "client_secret_post") {
        return undefined;
      }
      form.set("client_id", refresh.client_id);
      if (endpointAuth.type === "client_secret_post") {
        if (!nonEmpty(endpointAuth.client_secret)) {
          return undefined;
        }
        form.set("client_secret", endpointAuth.client_secret);
      }
    }
    const controller = new AbortController();
    const abortFromParent = () => controller.abort(parentSignal?.reason);
    if (parentSignal?.aborted === true) {
      abortFromParent();
    } else {
      parentSignal?.addEventListener("abort", abortFromParent, { once: true });
    }
    const timer = setTimeout(() => controller.abort(new Error("MCP OAuth refresh timed out.")), this.refreshHTTPTimeoutMs);
    if (typeof timer === "object" && timer !== null && "unref" in timer) {
      (timer as { unref: () => void }).unref();
    }
    try {
      const response = await this.fetchFn(refresh.token_endpoint, {
        method: "POST",
        headers,
        body: form,
        signal: controller.signal,
      });
      if (!response.ok) {
        return undefined;
      }
      const payload = await response.json() as { readonly access_token?: unknown; readonly refresh_token?: unknown; readonly expires_in?: unknown; readonly expires_at?: unknown };
      if (typeof payload.access_token !== "string" || payload.access_token.length === 0 || typeof payload.refresh_token !== "string" || payload.refresh_token.length === 0) {
        return undefined;
      }
      const expiresAt = typeof payload.expires_at === "string" && payload.expires_at.length > 0
        ? payload.expires_at
        : typeof payload.expires_in === "number" && payload.expires_in > 0
          ? new Date(this.now().getTime() + payload.expires_in * 1000).toISOString()
          : "";
      if (expiresAt === "") {
        return undefined;
      }
      return {
        accessToken: payload.access_token,
        refreshToken: payload.refresh_token,
        expiresAt,
      };
    } finally {
      clearTimeout(timer);
      parentSignal?.removeEventListener("abort", abortFromParent);
    }
  }
}

function expiryMovedForward(previousExpiresAt: string | undefined, currentExpiresAt: string | undefined): boolean {
  if (!nonEmpty(previousExpiresAt) || !nonEmpty(currentExpiresAt)) {
    return false;
  }
  const previous = Date.parse(previousExpiresAt);
  const current = Date.parse(currentExpiresAt);
  return Number.isFinite(previous) && Number.isFinite(current) && current > previous;
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

function materializeRefreshedAuth(current: CredentialAuth, refreshed: OAuthRefreshResult): CredentialAuth {
  return {
    ...current,
    access_token: refreshed.accessToken,
    expires_at: refreshed.expiresAt,
    refresh: current.refresh === undefined ? undefined : {
      ...current.refresh,
      refresh_token: refreshed.refreshToken,
    },
  };
}

function publicAuthFromSecret(auth: CredentialAuth): Record<string, unknown> {
  const output: Record<string, unknown> = {
    type: auth.type,
    mcp_server_url: auth.mcp_server_url,
  };
  if (nonEmpty(auth.expires_at)) {
    output.expires_at = auth.expires_at;
  }
  if (auth.refresh !== undefined) {
    const refresh: Record<string, unknown> = {};
    if (nonEmpty(auth.refresh.client_id)) {
      refresh.client_id = auth.refresh.client_id;
    }
    if (nonEmpty(auth.refresh.token_endpoint)) {
      refresh.token_endpoint = auth.refresh.token_endpoint;
    }
    if (nonEmpty(auth.refresh.scope)) {
      refresh.scope = auth.refresh.scope;
    }
    if (nonEmpty(auth.refresh.resource)) {
      refresh.resource = auth.refresh.resource;
    }
    if (auth.refresh.token_endpoint_auth?.type !== undefined) {
      refresh.token_endpoint_auth = { type: auth.refresh.token_endpoint_auth.type };
    }
    output.refresh = refresh;
  }
  return output;
}

function fail(error: "credential_required" | "ambiguous" | "undecryptable" | "expired" | "refresh_failed"): GitHubMcpCredentialResolution {
  return { ok: false, error };
}

function useToken(token: string, row: Pick<GitHubCredentialLockedRow, "id" | "vault_id">): GitHubMcpCredentialResolution {
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
  const key = await importAESKey(hexKey, ["decrypt"]);
  if (ciphertext.byteLength < 28) {
    throw new Error("ciphertext is too short");
  }
  const nonce = ciphertext.slice(0, 12);
  const sealed = ciphertext.slice(12);
  const plaintext = await crypto.subtle.decrypt({ name: "AES-GCM", iv: arrayBuffer(nonce), tagLength: 128 }, key, arrayBuffer(sealed));
  return new Uint8Array(plaintext);
}

async function encryptAES256GCM(plaintext: Uint8Array, hexKey: string): Promise<Uint8Array> {
  const key = await importAESKey(hexKey, ["encrypt"]);
  const nonce = new Uint8Array(12);
  crypto.getRandomValues(nonce);
  const sealed = await crypto.subtle.encrypt({ name: "AES-GCM", iv: arrayBuffer(nonce), tagLength: 128 }, key, arrayBuffer(plaintext));
  const output = new Uint8Array(nonce.byteLength + sealed.byteLength);
  output.set(nonce, 0);
  output.set(new Uint8Array(sealed), nonce.byteLength);
  return output;
}

async function importAESKey(hexKey: string, usages: KeyUsage[]): Promise<CryptoKey> {
  const keyBytes = bytesFromHex(hexKey);
  if (keyBytes.byteLength !== 32) {
    throw new Error("AES-256-GCM key must be 32 bytes");
  }
  return await crypto.subtle.importKey("raw", arrayBuffer(keyBytes), "AES-GCM", false, usages);
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
