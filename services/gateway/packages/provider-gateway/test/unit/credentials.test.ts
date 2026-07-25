import { describe, expect, test } from "bun:test";
import {
  ProviderRequestKind,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import {
  CachedPlatformCredentialPool,
  ProviderCredentialResolver,
  SQLGatewayCredentialStore,
} from "../../src/providers/credentials.js";
import { encryptAES256GCM } from "../../src/providers/crypto.js";
import type {
  GatewayCredentialSQL,
  GatewayCredentialStore,
  PlatformCredentialPool,
  SessionProviderAuthRow,
} from "../../src/providers/credentials.js";
import type { ProviderRequest } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { EncryptedPlatformProviderKeyRow, ProviderFailureClassification } from "../../src/providers/pool.js";

const MasterKeyHex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

describe("Gateway credential SQL store", () => {
  test("loads only active session provider auth and platform key rows through read queries", async () => {
    const encrypted = await encryptedAuth({
      type: "provider_api_key",
      provider_id: "anthropic",
      access_mode: "user_api_key",
      token: "sk-session",
    });
    const sqlCalls: string[] = [];
    const sql: GatewayCredentialSQL = async <T = unknown>(strings: TemplateStringsArray): Promise<T> => {
      const query = strings.join("?");
      sqlCalls.push(query);
      if (query.includes("set_config")) {
        return [] as T;
      }
      if (query.includes("session_provider_auth")) {
        return [
          {
            provider_id: "anthropic",
            vault_id: "vlt_1",
            credential_id: "cred_1",
            access_mode: "user_api_key",
            auth_type: "provider_api_key",
            credential_provider_id: "anthropic",
            credential_access_mode: "user_api_key",
            encrypted_auth: encrypted,
            archived_at: null,
            revoked_at: null,
          },
        ] as T;
      }
      return [
        {
          key_id: "pfk_anthropic_1",
          provider_id: "anthropic",
          encrypted_key: await encryptedPlatformKey("sk-platform"),
          weight: 1,
          priority: 0,
          cache_scope: "anthropic-workspace",
          status: "active",
          disabled_reason: null,
          updated_at: "2026-07-01T00:00:00Z",
        },
      ] as T;
    };
    sql.begin = async <T>(fn: (tx: GatewayCredentialSQL) => Promise<T>): Promise<T> => await fn(sql);
    const store = new SQLGatewayCredentialStore(sql);

    await expect(store.loadActiveSessionProviderAuth({ workspaceId: "wksp_1", sessionId: "sesn_1" })).resolves.toMatchObject([
      {
        providerId: "anthropic",
        credentialAuthType: "provider_api_key",
        encryptedAuth: encrypted,
        archived: false,
        revoked: false,
      },
    ]);
    await expect(store.loadPlatformProviderKeyRows()).resolves.toMatchObject([
      {
        keyId: "pfk_anthropic_1",
        providerId: "anthropic",
        status: "active",
      },
    ]);

    expect(sqlCalls).toHaveLength(3);
    expect(sqlCalls[0]).toContain("set_config('tetral.workspace_id'");
    expect(sqlCalls.join("\n")).toContain("SELECT");
    expect(sqlCalls.join("\n")).toContain("c.vault_id = spa.vault_id");
    expect(sqlCalls.join("\n")).not.toMatch(/\b(?:INSERT|UPDATE|DELETE)\b/);
  });
});

describe("Gateway provider credential resolver", () => {
  test("resolves an explicit session API key without consulting platform access", async () => {
    const platformPool = new RecordingPlatformPool();
    const resolver = new ProviderCredentialResolver({
      store: new MemoryCredentialStore({
        sessionRows: [
          await sessionRow({
            providerId: "anthropic",
            authType: "provider_api_key",
            accessMode: "user_api_key",
            auth: {
              type: "provider_api_key",
              provider_id: "anthropic",
              access_mode: "user_api_key",
              token: "sk-session-anthropic",
            },
          }),
        ],
      }),
      platformPool,
      masterKeyHex: MasterKeyHex,
    });

    await expect(resolver.resolve(request("anthropic", "claude-opus-4-8"))).resolves.toEqual({
      ok: true,
      credential: {
        source: "session",
        authType: "provider_api_key",
        providerId: "anthropic",
        supplyMode: "anthropic-api-key",
        vaultId: "vlt_test",
        credentialId: "cred_test",
        accessMode: "user_api_key",
        apiKey: "sk-session-anthropic",
      },
    });
    expect(platformPool.calls).toEqual([]);
  });

  test("resolves an explicit OpenAI OAuth credential without using the platform pool", async () => {
    const platformPool = new RecordingPlatformPool();
    const resolver = new ProviderCredentialResolver({
      store: new MemoryCredentialStore({
        sessionRows: [
          await sessionRow({
            providerId: "openai",
            authType: "provider_oauth",
            accessMode: "oauth",
            auth: {
              type: "provider_oauth",
              provider_id: "openai",
              access_mode: "oauth",
              access_token: "oauth-access",
              refresh_token: "oauth-refresh",
              expires_at: "2026-07-03T00:00:00Z",
              account_id: "acct_1",
            },
          }),
        ],
      }),
      platformPool,
      masterKeyHex: MasterKeyHex,
    });

    await expect(resolver.resolve(request("openai", "gpt-5.5"))).resolves.toMatchObject({
      ok: true,
      credential: {
        source: "session",
        authType: "provider_oauth",
        providerId: "openai",
        supplyMode: "openai-chatgpt-oauth",
        accessToken: "oauth-access",
        refreshToken: "oauth-refresh",
      },
    });
    expect(platformPool.calls).toEqual([]);
  });

  test("incomplete OpenAI OAuth credentials fail closed without platform fallback", async () => {
    const baseAuth = {
      type: "provider_oauth",
      provider_id: "openai",
      access_mode: "oauth",
      access_token: "oauth-access",
      refresh_token: "oauth-refresh",
      expires_at: "2026-07-03T00:00:00Z",
      account_id: "acct_1",
    };
    for (const testCase of [
      { name: "missing refresh", auth: { ...baseAuth, refresh_token: "" } },
      { name: "missing expiry", auth: { ...baseAuth, expires_at: "" } },
      { name: "malformed expiry", auth: { ...baseAuth, expires_at: "not a date" } },
      { name: "missing account", auth: { ...baseAuth, account_id: "" } },
    ]) {
      const platformPool = new RecordingPlatformPool();
      const resolver = new ProviderCredentialResolver({
        store: new MemoryCredentialStore({
          sessionRows: [
            await sessionRow({
              providerId: "openai",
              authType: "provider_oauth",
              accessMode: "oauth",
              auth: testCase.auth,
            }),
          ],
        }),
        platformPool,
        masterKeyHex: MasterKeyHex,
      });

      await expect(resolver.resolve(request("openai", "gpt-5.5"))).resolves.toMatchObject({
        ok: false,
        error: { code: "credential_unavailable", retryable: false, fatal: true },
      });
      expect(platformPool.calls, testCase.name).toEqual([]);
    }
  });

  test("explicit bad session credential fails closed and never falls back to platform access", async () => {
    const platformPool = new RecordingPlatformPool();
    const resolver = new ProviderCredentialResolver({
      store: new MemoryCredentialStore({
        sessionRows: [
          await sessionRow({
            providerId: "anthropic",
            credentialProviderId: "openai",
            authType: "provider_api_key",
            accessMode: "user_api_key",
            auth: {
              type: "provider_api_key",
              provider_id: "openai",
              access_mode: "user_api_key",
              token: "sk-wrong-provider",
            },
          }),
        ],
      }),
      platformPool,
      masterKeyHex: MasterKeyHex,
    });

    await expect(resolver.resolve(request("anthropic", "claude-opus-4-8"))).resolves.toMatchObject({
      ok: false,
      error: { code: "credential_unavailable", retryable: false, fatal: true },
    });
    expect(platformPool.calls).toEqual([]);
  });

  test("missing explicit session credential target fails closed and never falls back to platform access", async () => {
    const platformPool = new RecordingPlatformPool();
    const resolver = new ProviderCredentialResolver({
      store: new MemoryCredentialStore({
        sessionRows: [{
          providerId: "openai",
          vaultId: "vlt_test",
          credentialId: "cred_missing",
          accessMode: "api_key",
          archived: false,
          revoked: false,
        }],
      }),
      platformPool,
      masterKeyHex: MasterKeyHex,
    });

    await expect(resolver.resolve(request("openai", "gpt-5.5"))).resolves.toMatchObject({
      ok: false,
      error: { code: "credential_unavailable", retryable: false, fatal: true },
    });
    expect(platformPool.calls).toEqual([]);
  });

  test("corrupt explicit session credential auth fails closed and never falls back to platform access", async () => {
    const platformPool = new RecordingPlatformPool();
    const resolver = new ProviderCredentialResolver({
      store: new MemoryCredentialStore({
        sessionRows: [{
          providerId: "openai",
          vaultId: "vlt_test",
          credentialId: "cred_corrupt",
          accessMode: "api_key",
          credentialAuthType: "provider_api_key",
          credentialProviderId: "openai",
          credentialAccessMode: "api_key",
          encryptedAuth: new Uint8Array([1, 2, 3]),
          archived: false,
          revoked: false,
        }],
      }),
      platformPool,
      masterKeyHex: MasterKeyHex,
    });

    await expect(resolver.resolve(request("openai", "gpt-5.5"))).resolves.toMatchObject({
      ok: false,
      error: { code: "credential_unavailable", retryable: false, fatal: true },
    });
    expect(platformPool.calls).toEqual([]);
  });

  test("revoked or archived explicit session credentials fail closed without platform fallback", async () => {
    const revokedPlatformPool = new RecordingPlatformPool();
    const revokedResolver = new ProviderCredentialResolver({
      store: new MemoryCredentialStore({
        sessionRows: [
          await sessionRow({
            providerId: "anthropic",
            authType: "provider_api_key",
            accessMode: "user_api_key",
            revoked: true,
            auth: {
              type: "provider_api_key",
              provider_id: "anthropic",
              access_mode: "user_api_key",
              token: "sk-revoked",
            },
          }),
        ],
      }),
      platformPool: revokedPlatformPool,
      masterKeyHex: MasterKeyHex,
    });
    const archivedPlatformPool = new RecordingPlatformPool();
    const archivedResolver = new ProviderCredentialResolver({
      store: new MemoryCredentialStore({
        sessionRows: [
          await sessionRow({
            providerId: "anthropic",
            authType: "provider_api_key",
            accessMode: "user_api_key",
            archived: true,
            auth: {
              type: "provider_api_key",
              provider_id: "anthropic",
              access_mode: "user_api_key",
              token: "sk-archived",
            },
          }),
        ],
      }),
      platformPool: archivedPlatformPool,
      masterKeyHex: MasterKeyHex,
    });

    await expect(revokedResolver.resolve(request("anthropic", "claude-opus-4-8"))).resolves.toMatchObject({
      ok: false,
      error: { code: "credential_unavailable", retryable: false, fatal: true },
    });
    await expect(archivedResolver.resolve(request("anthropic", "claude-opus-4-8"))).resolves.toMatchObject({
      ok: false,
      error: { code: "credential_unavailable", retryable: false, fatal: true },
    });
    expect(revokedPlatformPool.calls).toEqual([]);
    expect(archivedPlatformPool.calls).toEqual([]);
  });

  test("uses platform-hosted access only for hosted providers and fails closed otherwise", async () => {
    const platformPool = new RecordingPlatformPool();
    const resolver = new ProviderCredentialResolver({
      store: new MemoryCredentialStore({ sessionRows: [] }),
      platformPool,
      masterKeyHex: MasterKeyHex,
    });

    await expect(resolver.resolve(request("anthropic", "claude-opus-4-8"))).resolves.toMatchObject({
      ok: true,
      credential: { source: "platform", providerId: "anthropic", supplyMode: "anthropic-api-key" },
    });
    for (const sessionOnlyModel of [
      { providerId: "moonshotai", modelId: "kimi-k3" },
      { providerId: "zai", modelId: "glm-5.2" },
    ]) {
      await expect(resolver.resolve(request(sessionOnlyModel.providerId, sessionOnlyModel.modelId))).resolves.toMatchObject({
        ok: false,
        error: { code: "credential_required", retryable: false },
      });
    }
    expect(platformPool.calls).toEqual(["anthropic"]);
  });

  test("compaction resolves credentials exactly like the agent while reviewers remain platform-owned", async () => {
    const platformPool = new RecordingPlatformPool();
    const store = new MemoryCredentialStore({
      sessionRows: [
        await sessionRow({
          providerId: "anthropic",
          authType: "provider_api_key",
          accessMode: "user_api_key",
          auth: {
            type: "provider_api_key",
            provider_id: "anthropic",
            access_mode: "user_api_key",
            token: "sk-session-ignored",
          },
        }),
      ],
    });
    const resolver = new ProviderCredentialResolver({ store, platformPool, masterKeyHex: MasterKeyHex });

    await expect(resolver.resolve(request("anthropic", "claude-opus-4-8", ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY))).resolves.toMatchObject({
      ok: true,
      credential: {
        source: "session",
        apiKey: "sk-session-ignored",
      },
    });
    await expect(resolver.resolve(request("anthropic", "claude-opus-4-8", ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER))).resolves.toMatchObject({
      ok: true,
      credential: { source: "platform" },
    });
    await expect(resolver.resolve(request(
      "anthropic",
      "claude-opus-4-8",
      ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER_COMPACTION,
    ))).resolves.toMatchObject({
      ok: true,
      credential: { source: "platform" },
    });
    expect(store.sessionReadCount).toBe(1);
    expect(platformPool.calls).toEqual(["anthropic", "anthropic"]);
  });
});

describe("Gateway cached platform credential pool", () => {
  test("caches table reads for 30s and refreshes decrypted keys without clearing quarantine memory", async () => {
    let now = 1_000;
    const store = new MemoryCredentialStore({
      platformRows: [await platformRow("pfk_old", "anthropic", "sk-old")],
    });
    const pool = new CachedPlatformCredentialPool({
      store,
      masterKeyHex: MasterKeyHex,
      now: () => now,
      cacheTtlMs: 30_000,
      poolOptions: { now: () => now, random: () => 0 },
    });

    await expect(pool.select("anthropic")).resolves.toMatchObject({ ok: true, key: { keyId: "pfk_old" } });
    await expect(pool.select("anthropic")).resolves.toMatchObject({ ok: true, key: { keyId: "pfk_old" } });
    expect(store.platformReadCount).toBe(1);

    pool.recordFailure("pfk_old", quarantineClassification());
    store.platformRows = [
      await platformRow("pfk_old", "anthropic", "sk-old"),
      await platformRow("pfk_new", "anthropic", "sk-new"),
    ];
    now += 30_001;

    await expect(pool.select("anthropic")).resolves.toMatchObject({ ok: true, key: { keyId: "pfk_new" } });
    expect(store.platformReadCount).toBe(2);
  });

  test("refuses startup warm when active platform keys mix cache scopes", async () => {
    const pool = new CachedPlatformCredentialPool({
      store: new MemoryCredentialStore({
        platformRows: [
          await platformRow("pfk_a", "anthropic", "sk-a", "scope_a"),
          await platformRow("pfk_b", "anthropic", "sk-b", "scope_b"),
        ],
      }),
      masterKeyHex: MasterKeyHex,
    });

    await expect(pool.warm()).rejects.toThrow("cache_scope");
  });
});

class MemoryCredentialStore implements GatewayCredentialStore {
  sessionReadCount = 0;
  platformReadCount = 0;
  platformRows: readonly EncryptedPlatformProviderKeyRow[];

  constructor(private readonly options: {
    readonly sessionRows?: readonly SessionProviderAuthRow[] | undefined;
    readonly platformRows?: readonly EncryptedPlatformProviderKeyRow[] | undefined;
  }) {
    this.platformRows = options.platformRows ?? [];
  }

  async loadActiveSessionProviderAuth(): Promise<readonly SessionProviderAuthRow[]> {
    this.sessionReadCount += 1;
    return this.options.sessionRows ?? [];
  }

  async loadPlatformProviderKeyRows(): Promise<readonly EncryptedPlatformProviderKeyRow[]> {
    this.platformReadCount += 1;
    return this.platformRows;
  }
}

class RecordingPlatformPool implements PlatformCredentialPool {
  readonly calls: string[] = [];

  async select(providerId: Parameters<PlatformCredentialPool["select"]>[0]) {
    this.calls.push(providerId);
    return {
      ok: true as const,
      key: {
        keyId: `pfk_${providerId}`,
        providerId,
        key: `sk-platform-${providerId}`,
        weight: 1,
        priority: 0,
        cacheScope: `${providerId}-scope`,
      },
    };
  }
}

function request(
  providerId: string,
  modelId: string,
  requestKind = ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
): Pick<ProviderRequest, "workspaceId" | "sessionId" | "requestKind" | "model"> {
  return {
    workspaceId: "wksp_1",
    sessionId: "sesn_1",
    requestKind,
    model: { providerId, modelId, variant: "" },
  };
}

async function sessionRow(input: {
  readonly providerId: SessionProviderAuthRow["providerId"];
  readonly credentialProviderId?: string | undefined;
  readonly authType: NonNullable<SessionProviderAuthRow["credentialAuthType"]>;
  readonly accessMode: string;
  readonly auth: unknown;
  readonly archived?: boolean | undefined;
  readonly revoked?: boolean | undefined;
}): Promise<SessionProviderAuthRow> {
  return {
    providerId: input.providerId,
    vaultId: "vlt_test",
    credentialId: "cred_test",
    accessMode: input.accessMode,
    credentialAuthType: input.authType,
    credentialProviderId: input.credentialProviderId ?? input.providerId,
    credentialAccessMode: input.accessMode,
    encryptedAuth: await encryptedAuth(input.auth),
    archived: input.archived ?? false,
    revoked: input.revoked ?? false,
  };
}

async function platformRow(
  keyId: string,
  providerId: EncryptedPlatformProviderKeyRow["providerId"],
  plaintext: string,
  cacheScope = `${providerId}-scope`,
): Promise<EncryptedPlatformProviderKeyRow> {
  return {
    keyId,
    providerId,
    encryptedKey: await encryptedPlatformKey(plaintext),
    weight: 1,
    priority: 0,
    cacheScope,
    status: "active",
    updatedAt: "2026-07-01T00:00:00Z",
  };
}

async function encryptedAuth(auth: unknown): Promise<Uint8Array> {
  return await encryptAES256GCM(new TextEncoder().encode(JSON.stringify(auth)), MasterKeyHex, () => new Uint8Array(12).fill(9));
}

async function encryptedPlatformKey(value: string): Promise<Uint8Array> {
  return await encryptAES256GCM(new TextEncoder().encode(value), MasterKeyHex, () => new Uint8Array(12).fill(5));
}

function quarantineClassification(): ProviderFailureClassification {
  return {
    action: "quarantine",
    providerError: {
      code: "provider_key_unavailable",
      message: "Provider key is not usable.",
      retryable: false,
      fatal: true,
      statusCode: 401,
    },
  };
}
