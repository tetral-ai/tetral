/**
 * @packageDocumentation
 * Rotates due OpenAI subscription OAuth credentials through a SQL-backed,
 * fail-closed credential update path.
 *
 * The writer holds a row lock from credential re-read through the issuer call
 * and encrypted write-back, so only one replica refreshes a credential at a
 * time. The update also compares the previously read encrypted bytes; a missed
 * comparison permits one committed-state re-read and never a refresh retry.
 * Redirects, malformed issuer responses, unusable rows, stale refresh material
 * on a still-due row, and persistence failures all require re-authorization.
 *
 * `command.ts` constructs the writer and `clients.ts` invokes it before a
 * due credential reaches the provider. The implementation calls the
 * credential SQL boundary, the shared AES-GCM codec, and the OpenAI token
 * endpoint constants, and returns usable plaintext OAuth material only to the
 * provider client.
 */
import type { FetchFunction } from "@ai-sdk/provider-utils";
import { decryptAES256GCM, encryptAES256GCM } from "./crypto.js";
import { OpenAICodexOAuthClientId, OpenAICodexOAuthTokenEndpoint } from "./openai-oauth.js";
import type { ResolvedSessionOAuthCredential } from "./credentials.js";
import { setWorkspaceRLS } from "./credentials.js";
import type { GatewayCredentialSQL } from "./credentials.js";

/** Time before expiry at which a credential is treated as due for refresh. */
export const OpenAIOAuthRefreshSkewMs = 60_000;
/**
 * Accepted redirect count for token refreshes; every redirect response fails
 * closed instead of being followed.
 */
export const MAX_FOLLOWED_REDIRECTS = 0;

/** Outcome returned to the provider client after checking or rotating OAuth material. */
export type OpenAIOAuthCredentialRefreshResult =
  | { readonly ok: true; readonly credential: ResolvedSessionOAuthCredential }
  | { readonly ok: false; readonly error: "credential_required" };

/** Refresh boundary consumed by the provider client registry. */
export interface OpenAIOAuthCredentialRefreshWriter {
  readonly refreshOpenAIOAuthCredential: (input: {
    readonly workspaceId: string;
    readonly credential: ResolvedSessionOAuthCredential;
    readonly abortSignal: AbortSignal;
  }) => Promise<OpenAIOAuthCredentialRefreshResult>;
}

interface ProviderOAuthCredentialSQLRow {
  readonly id: string;
  readonly auth_type: string;
  readonly provider_id: string | null;
  readonly access_mode: string | null;
  readonly encrypted_auth: unknown;
  readonly archived_at: string | null;
  readonly revoked_at: string | null;
}

interface ProviderOAuthCredentialUpdatedRow {
  readonly id: string;
}

interface StoredProviderOAuthAuth {
  readonly type?: string | undefined;
  readonly provider_id?: string | undefined;
  readonly access_mode?: string | undefined;
  readonly access_token?: string | undefined;
  readonly refresh_token?: string | undefined;
  readonly expires_at?: string | undefined;
  readonly account_id?: string | undefined;
}

interface TokenEndpointResponse {
  readonly access_token?: unknown;
  readonly refresh_token?: unknown;
  readonly expires_in?: unknown;
  readonly expires_at?: unknown;
}

/** Persistence, encryption, transport, and clock dependencies for OAuth rotation. */
export interface SQLOpenAIOAuthCredentialRefreshWriterOptions {
  readonly sql: GatewayCredentialSQL;
  readonly masterKeyHex: string;
  readonly fetch?: FetchFunction | undefined;
  readonly now?: () => number;
}

// OpenAI OAuth credential rotation — the ONE durable-write path in this package.
// A `SELECT ... FOR UPDATE` row lock is held across BOTH the issuer refresh HTTP
// call and the write-back, so at most one refresh per credential row is in flight
// across all replicas: a replica that blocks on the lock re-reads afterward and,
// if the row is now fresh, uses it without issuing its own refresh (provider
// refresh tokens rotate on use, so two concurrent refreshes would invalidate each
// other). The write-back CAS is byte-equality on the encrypted_auth read under
// the lock; because AES-GCM uses a random nonce, byte-equality is strictly
// stronger than token-value equality (any intervening write changes the bytes).
//
//   | state                | meaning                          | next                                    |
//   | -------------------- | -------------------------------- | --------------------------------------- |
//   | load-for-update      | row locked, decrypted, verified  | not-due -> return current               |
//   |                      | against the caller credential    | due -> refresh-with-issuer              |
//   | refresh-with-issuer  | POST to the token endpoint        | success -> write-back-CAS               |
//   |                      |                                  | failure -> credential_required          |
//   | write-back-CAS       | UPDATE guarded by previous        | 1 row -> return rotated                 |
//   |                      | encrypted_auth byte-equality      | 0 rows (miss) -> reread-committed        |
//   | reread-committed     | one bounded re-read of the        | fresh -> return that credential         |
//   |                      | committed row after a CAS miss    | still due -> fail closed (credential_   |
//   |                      |                                  | required); no refresh-retry loop        |
/**
 * PostgreSQL OAuth rotation writer with cross-replica row-lock serialization
 * and encrypted-byte compare-and-swap protection.
 */
export class SQLOpenAIOAuthCredentialRefreshWriter implements OpenAIOAuthCredentialRefreshWriter {
  private readonly fetchImpl: FetchFunction;
  private readonly now: () => number;

  constructor(private readonly options: SQLOpenAIOAuthCredentialRefreshWriterOptions) {
    this.fetchImpl = options.fetch ?? globalThis.fetch.bind(globalThis);
    this.now = options.now ?? Date.now;
  }

  /** Returns current material or rotates a due credential under its row lock. */
  async refreshOpenAIOAuthCredential(input: {
    readonly workspaceId: string;
    readonly credential: ResolvedSessionOAuthCredential;
    readonly abortSignal: AbortSignal;
  }): Promise<OpenAIOAuthCredentialRefreshResult> {
    if (input.credential.refreshToken === undefined || input.credential.refreshToken.length === 0 || this.options.sql.begin === undefined) {
      return { ok: false, error: "credential_required" };
    }
    try {
      const result: OpenAIOAuthCredentialRefreshResult | { readonly ok: false; readonly error: "cas_miss" } = await this.options.sql.begin(async (tx) => {
        await setWorkspaceRLS(tx, input.workspaceId);
        const row = await this.loadCredentialForUpdate(tx, input.workspaceId, input.credential);
        if (row === undefined || !providerOAuthRowUsable(row, input.credential)) {
          return { ok: false, error: "credential_required" };
        }
        const encryptedAuth = databaseBytes(row.encrypted_auth);
        const auth = await this.decryptAuth(encryptedAuth);
        if (!storedAuthMatchesCredential(auth, input.credential)) {
          return { ok: false, error: "credential_required" };
        }
        const current = credentialFromAuth(input.credential, auth);
        if (!openAIOAuthCredentialRefreshDue(current.expiresAt, this.now())) {
          return { ok: true, credential: current };
        }
        if (auth.refresh_token !== input.credential.refreshToken) {
          return { ok: false, error: "credential_required" };
        }
        const rotated = await this.refreshWithIssuer(current, input.abortSignal);
        if (rotated === undefined) {
          return { ok: false, error: "credential_required" };
        }
        const updated = await this.writeCredential(tx, input.workspaceId, current, rotated, encryptedAuth);
        if (!updated) {
          return { ok: false as const, error: "cas_miss" as const };
        }
        return { ok: true as const, credential: rotated };
      });
      if (!result.ok && result.error === "cas_miss") {
        return await this.readFreshCommittedCredential(input.workspaceId, input.credential);
      }
      return result;
    } catch {
      return { ok: false, error: "credential_required" };
    }
  }

  private async readFreshCommittedCredential(
    workspaceId: string,
    credential: ResolvedSessionOAuthCredential,
  ): Promise<OpenAIOAuthCredentialRefreshResult> {
    const read = async (sql: GatewayCredentialSQL): Promise<OpenAIOAuthCredentialRefreshResult> => {
      await setWorkspaceRLS(sql, workspaceId);
      const rows = await sql<readonly ProviderOAuthCredentialSQLRow[]>`
        SELECT id, auth_type, provider_id, access_mode, encrypted_auth, archived_at, revoked_at
          FROM credentials
         WHERE workspace_id = ${workspaceId}
           AND vault_id = ${credential.vaultId}
           AND id = ${credential.credentialId}
      `;
      const row = rows[0];
      if (row === undefined || !providerOAuthRowUsable(row, credential)) {
        return { ok: false, error: "credential_required" };
      }
      const auth = await this.decryptAuth(databaseBytes(row.encrypted_auth));
      if (!storedAuthMatchesCredential(auth, credential)) {
        return { ok: false, error: "credential_required" };
      }
      const fresh = credentialFromAuth(credential, auth);
      return openAIOAuthCredentialRefreshDue(fresh.expiresAt, this.now())
        ? { ok: false, error: "credential_required" }
        : { ok: true, credential: fresh };
    };
    if (this.options.sql.begin !== undefined) {
      return await this.options.sql.begin(read);
    }
    return await read(this.options.sql);
  }

  private async loadCredentialForUpdate(
    tx: GatewayCredentialSQL,
    workspaceId: string,
    credential: ResolvedSessionOAuthCredential,
  ): Promise<ProviderOAuthCredentialSQLRow | undefined> {
    const rows = await tx<readonly ProviderOAuthCredentialSQLRow[]>`
      SELECT id, auth_type, provider_id, access_mode, encrypted_auth, archived_at, revoked_at
        FROM credentials
       WHERE workspace_id = ${workspaceId}
         AND vault_id = ${credential.vaultId}
         AND id = ${credential.credentialId}
       FOR UPDATE
    `;
    return rows[0];
  }

  private async decryptAuth(encryptedAuth: Uint8Array): Promise<StoredProviderOAuthAuth> {
    const plaintext = await decryptAES256GCM(encryptedAuth, this.options.masterKeyHex);
    return JSON.parse(new TextDecoder().decode(plaintext)) as StoredProviderOAuthAuth;
  }

  private async refreshWithIssuer(
    credential: ResolvedSessionOAuthCredential,
    abortSignal: AbortSignal,
  ): Promise<ResolvedSessionOAuthCredential | undefined> {
    const refreshToken = credential.refreshToken;
    if (refreshToken === undefined || refreshToken.length === 0) {
      return undefined;
    }
    const body = new URLSearchParams({
      client_id: OpenAICodexOAuthClientId,
      grant_type: "refresh_token",
      refresh_token: refreshToken,
    });
    let response: Response;
    try {
      response = await this.fetchImpl(OpenAICodexOAuthTokenEndpoint, {
        method: "POST",
        headers: { "content-type": "application/x-www-form-urlencoded" },
        body,
        signal: abortSignal,
        redirect: "manual",
      });
    } catch {
      return undefined;
    }
    if (response.status >= 300 && response.status <= 399) {
      try {
        await response.body?.cancel();
      } catch {
        // The response is untrusted and is discarded regardless of cancel outcome.
      }
      return undefined;
    }
    if (!response.ok) {
      return undefined;
    }
    let payload: TokenEndpointResponse;
    try {
      payload = await response.json() as TokenEndpointResponse;
    } catch {
      return undefined;
    }
    if (typeof payload.access_token !== "string" || payload.access_token.length === 0 || typeof payload.refresh_token !== "string" || payload.refresh_token.length === 0) {
      return undefined;
    }
    const expiresAt = tokenExpiresAt(payload, this.now());
    if (expiresAt === undefined) {
      return undefined;
    }
    return {
      ...credential,
      accessToken: payload.access_token,
      refreshToken: payload.refresh_token,
      expiresAt,
    };
  }

  private async writeCredential(
    tx: GatewayCredentialSQL,
    workspaceId: string,
    previous: ResolvedSessionOAuthCredential,
    rotated: ResolvedSessionOAuthCredential,
    previousEncryptedAuth: Uint8Array,
  ): Promise<boolean> {
    const auth = storedAuthFromCredential(rotated);
    const encryptedAuth = await encryptAES256GCM(new TextEncoder().encode(JSON.stringify(auth)), this.options.masterKeyHex);
    const publicAuth = publicAuthFromStoredAuth(auth);
    const rows = await tx<readonly ProviderOAuthCredentialUpdatedRow[]>`
      UPDATE credentials
         SET encrypted_auth = ${encryptedAuth},
             auth_public_json = ${JSON.stringify(publicAuth)},
             provider_id = ${rotated.providerId},
             access_mode = ${rotated.accessMode},
             updated_at = ${new Date(this.now()).toISOString()}
       WHERE workspace_id = ${workspaceId}
         AND vault_id = ${rotated.vaultId}
         AND id = ${rotated.credentialId}
         AND auth_type = 'provider_oauth'
         AND provider_id = ${previous.providerId}
         AND access_mode = ${previous.accessMode}
         AND encrypted_auth = ${previousEncryptedAuth}
       RETURNING id
    `;
    return rows.length === 1;
  }
}

/**
 * Reports whether a parseable expiry falls within the refresh-skew window.
 * Missing or malformed expiries are not considered due by this predicate and
 * are rejected earlier when stored credential material is validated.
 */
export function openAIOAuthCredentialRefreshDue(expiresAt: string | undefined, nowMs: number = Date.now()): boolean {
  if (expiresAt === undefined || expiresAt.length === 0) {
    return false;
  }
  const expiresMs = Date.parse(expiresAt);
  return Number.isFinite(expiresMs) && expiresMs <= nowMs + OpenAIOAuthRefreshSkewMs;
}

function providerOAuthRowUsable(row: ProviderOAuthCredentialSQLRow, credential: ResolvedSessionOAuthCredential): boolean {
  return row.auth_type === "provider_oauth" &&
    row.provider_id === credential.providerId &&
    row.access_mode === credential.accessMode &&
    !nonEmptyString(row.archived_at) &&
    !nonEmptyString(row.revoked_at);
}

function storedAuthMatchesCredential(auth: StoredProviderOAuthAuth, credential: ResolvedSessionOAuthCredential): boolean {
  return auth.type === "provider_oauth" &&
    auth.provider_id === credential.providerId &&
    auth.access_mode === credential.accessMode &&
    nonEmptyString(auth.access_token) &&
    nonEmptyString(auth.refresh_token) &&
    nonEmptyString(auth.expires_at) &&
    nonEmptyString(auth.account_id) &&
    Number.isFinite(Date.parse(auth.expires_at));
}

function credentialFromAuth(base: ResolvedSessionOAuthCredential, auth: StoredProviderOAuthAuth): ResolvedSessionOAuthCredential {
  return {
    ...base,
    accessToken: auth.access_token ?? base.accessToken,
    refreshToken: auth.refresh_token ?? base.refreshToken,
    expiresAt: auth.expires_at ?? base.expiresAt,
    accountId: auth.account_id ?? base.accountId,
  };
}

function storedAuthFromCredential(credential: ResolvedSessionOAuthCredential): StoredProviderOAuthAuth {
  return {
    type: "provider_oauth",
    provider_id: credential.providerId,
    access_mode: credential.accessMode,
    access_token: credential.accessToken,
    refresh_token: credential.refreshToken,
    expires_at: credential.expiresAt,
    account_id: credential.accountId,
  };
}

function publicAuthFromStoredAuth(auth: StoredProviderOAuthAuth): StoredProviderOAuthAuth {
  return {
    type: auth.type,
    provider_id: auth.provider_id,
    access_mode: auth.access_mode,
    expires_at: auth.expires_at,
    account_id: auth.account_id,
  };
}

function tokenExpiresAt(payload: TokenEndpointResponse, nowMs: number): string | undefined {
  if (typeof payload.expires_at === "string" && Number.isFinite(Date.parse(payload.expires_at))) {
    return payload.expires_at;
  }
  if (typeof payload.expires_in !== "number" || !Number.isFinite(payload.expires_in) || payload.expires_in <= 0) {
    return undefined;
  }
  return new Date(nowMs + Math.floor(payload.expires_in * 1000)).toISOString();
}

function databaseBytes(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) {
    return value;
  }
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength));
  }
  throw new Error("database bytea value is not bytes");
}

function nonEmptyString(value: string | null | undefined): value is string {
  return typeof value === "string" && value.length > 0;
}
