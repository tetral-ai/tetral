/**
 * @packageDocumentation
 * Resolves a provider turn to one validated session credential or one selected
 * platform key, and supplies the SQL-backed readers and cached platform pool
 * used by that decision.
 *
 * A bound session credential is authoritative: ambiguous bindings or a missing,
 * revoked, archived, mismatched, or undecryptable joined credential fail closed
 * and never fall back to a platform key. Agent and compaction requests share the
 * session-first path, while approval-reviewer requests use platform credentials
 * exclusively. Decrypted session material leaves this module only in a
 * discriminated resolved credential for one provider call; decrypted platform
 * keys remain in the disposable process pool across calls until its loaded
 * snapshot replaces them.
 *
 * Startup wiring in `command.ts` constructs the store, pool, and resolver;
 * `service.ts` asks the resolver for each turn and records platform-key failures;
 * and `clients.ts` consumes the resolved credential. This module reads
 * PostgreSQL through the credential SQL interface and delegates catalog checks,
 * AES-GCM decryption, pool selection, and public error normalization to their
 * owning modules.
 */
import {
  ProviderRequestKind,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { normalizeProviderError } from "@tetral/gateway-lowering/src/errors.js";
import { lookupGatewayModel } from "./catalog.js";
import { decryptAES256GCM } from "./crypto.js";
import {
  decryptPlatformProviderKeyRows,
  PlatformKeyPool,
  PlatformKeyPoolConstants,
} from "./pool.js";
import type {
  ProviderRequest,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { ProviderErrorInput } from "@tetral/gateway-lowering/src/errors.js";
import type { GatewayModelCatalogEntry, GatewaySupplyMode, GatewaySupplyProviderId } from "./catalog.js";
import type {
  EncryptedPlatformProviderKeyRow,
  PlatformKeySelectionOptions,
  PlatformHostedProviderId,
  PlatformKeyPoolOptions,
  PlatformKeySelection,
  PlatformProviderKey,
  ProviderFailureClassification,
} from "./pool.js";

/**
 * Minimal tagged-template SQL surface shared by credential readers and the
 * OAuth rotation writer.
 *
 * Transaction-dependent operations inspect the optional `begin` capability.
 * The session reader returns no rows without it, while the OAuth rotation
 * writer rejects the refresh.
 */
export interface GatewayCredentialSQL {
  <T = unknown>(strings: TemplateStringsArray, ...values: unknown[]): PromiseLike<T>;
  begin?<T>(fn: (sql: GatewayCredentialSQL) => Promise<T>): Promise<T>;
}

/**
 * Read-only persistence boundary used by credential resolution and platform
 * pool refreshes.
 */
export interface GatewayCredentialStore {
  readonly loadActiveSessionProviderAuth: (input: {
    readonly workspaceId: string;
    readonly sessionId: string;
  }) => Promise<readonly SessionProviderAuthRow[]>;
  readonly loadPlatformProviderKeyRows: () => Promise<readonly EncryptedPlatformProviderKeyRow[]>;
}

/**
 * Joined session binding and Vault credential metadata returned by the store.
 * Optional credential fields represent a missing or invalid joined row so the
 * resolver can reject it without selecting another credential source.
 */
export interface SessionProviderAuthRow {
  readonly providerId: GatewaySupplyProviderId;
  readonly vaultId: string;
  readonly credentialId: string;
  readonly accessMode: string;
  readonly credentialAuthType?: "provider_api_key" | "provider_oauth" | undefined;
  readonly credentialProviderId?: string | undefined;
  readonly credentialAccessMode?: string | undefined;
  readonly encryptedAuth?: Uint8Array | undefined;
  readonly archived: boolean;
  readonly revoked: boolean;
}

/** Result of fail-closed credential resolution for one provider request. */
export type ProviderCredentialResolverResult =
  | { readonly ok: true; readonly credential: ResolvedProviderCredential }
  | { readonly ok: false; readonly error: ProviderErrorInput };

/**
 * Credential material accepted by provider clients after catalog, binding, and
 * stored-auth validation.
 *
 * The `source` and `authType` discriminants determine whether the caller reads
 * a session API key, OpenAI OAuth material, or a selected platform key.
 */
export type ResolvedProviderCredential =
  | ResolvedSessionAPIKeyCredential
  | ResolvedSessionOAuthCredential
  | ResolvedPlatformCredential;

/** A validated session-owned API key and its provider supply route. */
export interface ResolvedSessionAPIKeyCredential {
  readonly source: "session";
  readonly authType: "provider_api_key";
  readonly providerId: GatewaySupplyProviderId;
  readonly supplyMode: Exclude<GatewaySupplyMode, "openai-chatgpt-oauth">;
  readonly vaultId: string;
  readonly credentialId: string;
  readonly accessMode: string;
  readonly apiKey: string;
}

/**
 * Validated OpenAI subscription OAuth material loaded from a session-bound
 * Vault credential. The refresh writer returns the same shape after rotation.
 */
export interface ResolvedSessionOAuthCredential {
  readonly source: "session";
  readonly authType: "provider_oauth";
  readonly providerId: "openai";
  readonly supplyMode: "openai-chatgpt-oauth";
  readonly vaultId: string;
  readonly credentialId: string;
  readonly accessMode: "oauth";
  readonly accessToken: string;
  readonly refreshToken: string;
  readonly expiresAt: string;
  readonly accountId: string;
}

/** A platform-owned API key selected from the per-replica key pool. */
export interface ResolvedPlatformCredential {
  readonly source: "platform";
  readonly authType: "provider_api_key";
  readonly providerId: PlatformHostedProviderId;
  readonly supplyMode: Extract<GatewaySupplyMode, "anthropic-api-key" | "openai-api-key" | "deepseek-api-key">;
  readonly platformKey: PlatformProviderKey;
}

/** Dependencies required to resolve and decrypt credentials for provider calls. */
export interface ProviderCredentialResolverOptions {
  readonly store: GatewayCredentialStore;
  readonly platformPool: PlatformCredentialPool;
  readonly masterKeyHex: string;
}

/**
 * Selection and failure-observation boundary between request credential
 * resolution and the disposable per-replica platform key state machine.
 */
export interface PlatformCredentialPool {
  readonly select: (providerId: PlatformHostedProviderId, options?: PlatformKeySelectionOptions) => Promise<PlatformKeySelection>;
  readonly recordFailure?: (keyId: string, classification: ProviderFailureClassification) => void;
}

interface SessionCredentialAuth {
  readonly type?: string | undefined;
  readonly provider_id?: string | undefined;
  readonly access_mode?: string | undefined;
  readonly token?: string | undefined;
  readonly access_token?: string | undefined;
  readonly refresh_token?: string | undefined;
  readonly expires_at?: string | undefined;
  readonly account_id?: string | undefined;
}

interface SessionProviderAuthSQLRow {
  readonly provider_id: string;
  readonly vault_id: string;
  readonly credential_id: string;
  readonly access_mode: string;
  readonly auth_type?: string | null | undefined;
  readonly credential_provider_id?: string | null | undefined;
  readonly credential_access_mode?: string | null | undefined;
  readonly encrypted_auth?: unknown;
  readonly archived_at?: string | null | undefined;
  readonly revoked_at?: string | null | undefined;
}

interface PlatformProviderKeySQLRow {
  readonly key_id: string;
  readonly provider_id: string;
  readonly encrypted_key: unknown;
  readonly weight: number;
  readonly priority: number;
  readonly cache_scope: string;
  readonly status: string;
  readonly disabled_reason?: string | null | undefined;
  readonly updated_at: string;
}

/**
 * PostgreSQL implementation of the gateway's read-only credential store.
 * Session reads run with workspace row-level security and preserve ambiguity by
 * returning up to two bindings for fail-closed handling by the resolver.
 */
export class SQLGatewayCredentialStore implements GatewayCredentialStore {
  constructor(private readonly sql: GatewayCredentialSQL) {}

  // A session carries at most one bound provider credential. The query below
  // selects LIMIT 2 (not LIMIT 1) as cheap ambiguity detection: if two or more
  // matching session_provider_auth rows exist, the resolver sees rows.length > 1
  // and fails closed (credentialUnavailable) rather than silently picking one.
  /** Loads at most two active bindings so the resolver can reject ambiguity. */
  async loadActiveSessionProviderAuth(input: {
    readonly workspaceId: string;
    readonly sessionId: string;
  }): Promise<readonly SessionProviderAuthRow[]> {
    if (this.sql.begin === undefined) {
      return [];
    }
    const rows = await this.sql.begin(async (tx) => {
      await setWorkspaceRLS(tx, input.workspaceId);
      return await tx<readonly SessionProviderAuthSQLRow[]>`
        SELECT
          spa.provider_id,
          spa.vault_id,
          spa.credential_id,
          spa.access_mode,
          c.auth_type,
          c.provider_id AS credential_provider_id,
          c.access_mode AS credential_access_mode,
          c.encrypted_auth,
          c.archived_at,
          c.revoked_at
        FROM session_provider_auth spa
        LEFT JOIN credentials c
          ON c.workspace_id = spa.workspace_id
         AND c.vault_id = spa.vault_id
         AND c.id = spa.credential_id
        WHERE spa.workspace_id = ${input.workspaceId}
          AND spa.session_id = ${input.sessionId}
          AND spa.deleted_at IS NULL
        ORDER BY spa.updated_at DESC
        LIMIT 2
      `;
    });
    return rows.map(sessionProviderAuthRowFromSQL);
  }

  /** Loads the encrypted platform-key snapshot consumed by the cached pool. */
  async loadPlatformProviderKeyRows(): Promise<readonly EncryptedPlatformProviderKeyRow[]> {
    const rows = await this.sql<readonly PlatformProviderKeySQLRow[]>`
      SELECT
        key_id,
        provider_id,
        encrypted_key,
        weight,
        priority,
        cache_scope,
        status,
        disabled_reason,
        updated_at
      FROM platform_provider_keys
      ORDER BY provider_id, status, priority, key_id
    `;
    return rows.map(platformProviderKeyRowFromSQL);
  }
}

/** Sets the transaction-local workspace used by PostgreSQL row-level security. */
export async function setWorkspaceRLS(sql: GatewayCredentialSQL, workspaceId: string): Promise<void> {
  await sql`SELECT set_config('tetral.workspace_id', ${workspaceId}, true)`;
}

/**
 * Refreshes encrypted platform-key rows into a `PlatformKeyPool` and serves
 * selections from that pool for a bounded cache interval.
 *
 * Concurrent stale readers share one refresh promise. Cooldown and quarantine
 * observations remain in the underlying in-memory pool when database rows are
 * refreshed.
 */
export class CachedPlatformCredentialPool implements PlatformCredentialPool {
  private readonly pool: PlatformKeyPool;
  private readonly now: () => number;
  private cachedUntilMs = 0;
  private refreshPromise: Promise<void> | undefined;

  constructor(
    private readonly options: {
      readonly store: Pick<GatewayCredentialStore, "loadPlatformProviderKeyRows">;
      readonly masterKeyHex: string;
      readonly poolOptions?: PlatformKeyPoolOptions | undefined;
      readonly cacheTtlMs?: number | undefined;
      readonly now?: () => number;
    },
  ) {
    this.now = options.now ?? Date.now;
    this.pool = new PlatformKeyPool([], options.poolOptions);
  }

  /** Refreshes stale key data, then selects from the per-replica pool. */
  async select(providerId: PlatformHostedProviderId, options: PlatformKeySelectionOptions = {}): Promise<PlatformKeySelection> {
    await this.refreshIfStale();
    return this.pool.select(providerId, options);
  }

  /** Ensures the first platform-key snapshot is loaded before serving traffic. */
  async warm(): Promise<void> {
    await this.refreshIfStale();
  }

  /** Forwards a provider failure observation to the per-replica key pool. */
  recordFailure(keyId: string, classification: Parameters<PlatformKeyPool["recordFailure"]>[1]): void {
    this.pool.recordFailure(keyId, classification);
  }

  private async refreshIfStale(): Promise<void> {
    if (this.now() < this.cachedUntilMs) {
      return;
    }
    this.refreshPromise ??= this.refresh();
    try {
      await this.refreshPromise;
    } finally {
      this.refreshPromise = undefined;
    }
  }

  private async refresh(): Promise<void> {
    const rows = await this.options.store.loadPlatformProviderKeyRows();
    const keys = await decryptPlatformProviderKeyRows(rows, this.options.masterKeyHex);
    this.pool.replaceKeys(keys);
    this.cachedUntilMs = this.now() + (this.options.cacheTtlMs ?? PlatformKeyPoolConstants.poolReadCacheTtlMs);
  }
}

/**
 * Applies request-kind, model-catalog, session-binding, and platform-hosting
 * rules to resolve exactly one credential source for a provider turn.
 *
 * Agent and compaction requests treat an existing session binding as
 * authoritative, including when it is unusable, and select platform access
 * only when no binding exists. Approval-reviewer requests and compactions on
 * reviewer threads bypass session bindings and select platform credentials exclusively.
 */
export class ProviderCredentialResolver {
  constructor(private readonly options: ProviderCredentialResolverOptions) {}

  /** Resolves the credential source prescribed by the request kind and model. */
  async resolve(
    request: Pick<ProviderRequest, "workspaceId" | "sessionId" | "requestKind" | "model">,
    options: { readonly excludedPlatformKeyIds?: ReadonlySet<string> | undefined } = {},
  ): Promise<ProviderCredentialResolverResult> {
    const model = request.model;
    const entry = model === undefined ? undefined : lookupGatewayModel(model.providerId, model.modelId);
    if (entry === undefined) {
      return failClosed("provider_unavailable", "Provider model is not approved by the Gateway catalog.");
    }
    if (
      request.requestKind === ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER ||
      request.requestKind === ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER_COMPACTION
    ) {
      return await this.resolvePlatform(entry, options);
    }
    if (
      request.requestKind !== ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST &&
      request.requestKind !== ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY
    ) {
      return failClosed("provider_unavailable", "Provider request kind is not supported for credential resolution.");
    }

    const rows = await this.options.store.loadActiveSessionProviderAuth({
      workspaceId: request.workspaceId,
      sessionId: request.sessionId,
    });
    if (rows.length > 1) {
      return credentialUnavailable();
    }
    const row = rows[0];
    if (row === undefined) {
      return await this.resolvePlatform(entry, options);
    }
    return await this.resolveSession(row, entry);
  }

  /** Records a selected platform key's classified failure when supported. */
  recordPlatformFailure(keyId: string, classification: ProviderFailureClassification): void {
    this.options.platformPool.recordFailure?.(keyId, classification);
  }

  private async resolvePlatform(
    entry: GatewayModelCatalogEntry,
    options: { readonly excludedPlatformKeyIds?: ReadonlySet<string> | undefined },
  ): Promise<ProviderCredentialResolverResult> {
    if (!entry.platformHosted || !isPlatformHostedProviderId(entry.supplyProviderId)) {
      return failClosed("credential_required", "This provider requires an explicit session credential.");
    }
    const selection = await this.options.platformPool.select(entry.supplyProviderId, {
      excludeKeyIds: options.excludedPlatformKeyIds,
    });
    if (!selection.ok) {
      return { ok: false, error: selection.error };
    }
    const supplyMode = platformSupplyMode(entry.supplyProviderId);
    if (supplyMode === undefined) {
      return credentialUnavailable();
    }
    return {
      ok: true,
      credential: {
        source: "platform",
        authType: "provider_api_key",
        providerId: entry.supplyProviderId,
        supplyMode,
        platformKey: selection.key,
      },
    };
  }

  private async resolveSession(row: SessionProviderAuthRow, entry: GatewayModelCatalogEntry): Promise<ProviderCredentialResolverResult> {
    if (
      row.providerId !== entry.providerId ||
      row.credentialAuthType === undefined ||
      row.encryptedAuth === undefined ||
      row.archived ||
      row.revoked ||
      row.credentialProviderId !== entry.providerId ||
      row.credentialAccessMode !== row.accessMode
    ) {
      return credentialUnavailable();
    }
    let auth: SessionCredentialAuth;
    try {
      const plaintext = await decryptAES256GCM(row.encryptedAuth, this.options.masterKeyHex);
      auth = JSON.parse(new TextDecoder().decode(plaintext)) as SessionCredentialAuth;
    } catch {
      return credentialUnavailable();
    }
    if (auth.type !== row.credentialAuthType || auth.provider_id !== entry.providerId || auth.access_mode !== row.accessMode) {
      return credentialUnavailable();
    }
    if (row.credentialAuthType === "provider_api_key") {
      return this.resolveSessionAPIKey(row, entry, auth);
    }
    return this.resolveSessionOAuth(row, entry, auth);
  }

  private resolveSessionAPIKey(row: SessionProviderAuthRow, entry: GatewayModelCatalogEntry, auth: SessionCredentialAuth): ProviderCredentialResolverResult {
    if (typeof auth.token !== "string" || auth.token.length === 0) {
      return credentialUnavailable();
    }
    const supplyMode = apiKeySupplyMode(entry.supplyProviderId);
    if (supplyMode === undefined) {
      return credentialUnavailable();
    }
    return {
      ok: true,
      credential: {
        source: "session",
        authType: "provider_api_key",
        providerId: entry.supplyProviderId,
        supplyMode,
        vaultId: row.vaultId,
        credentialId: row.credentialId,
        accessMode: row.accessMode,
        apiKey: auth.token,
      },
    };
  }

  private resolveSessionOAuth(row: SessionProviderAuthRow, entry: GatewayModelCatalogEntry, auth: SessionCredentialAuth): ProviderCredentialResolverResult {
    if (
      entry.supplyProviderId !== "openai" ||
      row.accessMode !== "oauth" ||
      !nonEmptyString(auth.access_token) ||
      !nonEmptyString(auth.refresh_token) ||
      !nonEmptyString(auth.expires_at) ||
      !nonEmptyString(auth.account_id) ||
      !Number.isFinite(Date.parse(auth.expires_at))
    ) {
      return credentialUnavailable();
    }
    return {
      ok: true,
      credential: {
        source: "session",
        authType: "provider_oauth",
        providerId: "openai",
        supplyMode: "openai-chatgpt-oauth",
        vaultId: row.vaultId,
        credentialId: row.credentialId,
        accessMode: "oauth",
        accessToken: auth.access_token,
        refreshToken: auth.refresh_token,
        expiresAt: auth.expires_at,
        accountId: auth.account_id,
      },
    };
  }
}

function sessionProviderAuthRowFromSQL(row: SessionProviderAuthSQLRow): SessionProviderAuthRow {
  const providerId = gatewaySupplyProviderId(row.provider_id);
  if (providerId === undefined) {
    throw new Error("session provider auth row has unsupported provider_id");
  }
  const authType = row.auth_type === "provider_api_key" || row.auth_type === "provider_oauth" ? row.auth_type : undefined;
  return {
    providerId,
    vaultId: row.vault_id,
    credentialId: row.credential_id,
    accessMode: row.access_mode,
    credentialAuthType: authType,
    credentialProviderId: row.credential_provider_id ?? undefined,
    credentialAccessMode: row.credential_access_mode ?? undefined,
    encryptedAuth: row.encrypted_auth === undefined || row.encrypted_auth === null ? undefined : databaseBytes(row.encrypted_auth),
    archived: row.archived_at !== undefined && row.archived_at !== null && row.archived_at !== "",
    revoked: row.revoked_at !== undefined && row.revoked_at !== null && row.revoked_at !== "",
  };
}

function platformProviderKeyRowFromSQL(row: PlatformProviderKeySQLRow): EncryptedPlatformProviderKeyRow {
  const providerId = platformHostedProviderId(row.provider_id);
  if (providerId === undefined) {
    throw new Error("platform provider key row has unsupported provider_id");
  }
  if (row.status !== "active" && row.status !== "disabled") {
    throw new Error("platform provider key row has unsupported status");
  }
  return {
    keyId: row.key_id,
    providerId,
    encryptedKey: databaseBytes(row.encrypted_key),
    weight: row.weight,
    priority: row.priority,
    cacheScope: row.cache_scope,
    status: row.status,
    disabledReason: row.disabled_reason ?? undefined,
    updatedAt: row.updated_at,
  };
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

function credentialUnavailable(): ProviderCredentialResolverResult {
  return failClosed("credential_unavailable", "The selected provider credential is not usable.");
}

function failClosed(code: string, message: string): ProviderCredentialResolverResult {
  return {
    ok: false,
    error: normalizeProviderError({
      code,
      message,
      retryable: false,
      fatal: true,
      statusCode: 403,
    }),
  };
}

function gatewaySupplyProviderId(value: string): GatewaySupplyProviderId | undefined {
  return value === "anthropic" || value === "openai" || value === "deepseek" || value === "moonshotai" || value === "zai" ? value : undefined;
}

function platformHostedProviderId(value: string): PlatformHostedProviderId | undefined {
  return isPlatformHostedProviderId(value) ? value : undefined;
}

function isPlatformHostedProviderId(value: string): value is PlatformHostedProviderId {
  return value === "anthropic" || value === "openai" || value === "deepseek";
}

function apiKeySupplyMode(providerId: GatewaySupplyProviderId): ResolvedSessionAPIKeyCredential["supplyMode"] | undefined {
  switch (providerId) {
    case "anthropic":
      return "anthropic-api-key";
    case "openai":
      return "openai-api-key";
    case "deepseek":
      return "deepseek-api-key";
    case "moonshotai":
      return "moonshotai-code-api-key";
    case "zai":
      return "zai-coding-api-key";
  }
}

function platformSupplyMode(providerId: PlatformHostedProviderId): ResolvedPlatformCredential["supplyMode"] | undefined {
  switch (providerId) {
    case "anthropic":
      return "anthropic-api-key";
    case "openai":
      return "openai-api-key";
    case "deepseek":
      return "deepseek-api-key";
  }
}

function nonEmptyString(value: string | undefined): value is string {
  return typeof value === "string" && value.length > 0;
}
