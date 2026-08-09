import { describe, expect, test } from "bun:test";
import {
  MAX_FOLLOWED_REDIRECTS,
  OpenAIOAuthRefreshSkewMs,
  SQLOpenAIOAuthCredentialRefreshWriter,
  openAIOAuthCredentialRefreshDue,
} from "../../src/providers/openai-oauth-refresh.js";
import { OpenAICodexOAuthTokenEndpoint } from "../../src/providers/openai-oauth.js";
import { decryptAES256GCM, encryptAES256GCM } from "../../src/providers/crypto.js";
import type { ResolvedSessionOAuthCredential } from "../../src/providers/credentials.js";
import type { GatewayCredentialSQL } from "../../src/providers/credentials.js";
import { normalizeOpenAIOAuthExpiry, parseCanonicalOpenAIOAuthExpiry } from "../../src/providers/openai-oauth-expiry.js";

const MasterKeyHex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const Now = Date.parse("2026-07-06T12:00:00.000Z");
const NeverAbort = new AbortController().signal;

describe("OpenAI OAuth refresh boundary", () => {
  test("posts once to the contracted endpoint with manual redirect mode", async () => {
    const sql = await lockedCredentialSQL(await encryptedAuth(expiredStoredAuth()));
    const requests: Array<{ readonly url: string; readonly init: RequestInit | undefined }> = [];
    const writer = new SQLOpenAIOAuthCredentialRefreshWriter({
      sql,
      masterKeyHex: MasterKeyHex,
      now: () => Now,
      fetch: pinnedOpenAIIssuerFetch(async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
        requests.push({ url: String(input), init });
        return Response.json({
          access_token: "oauth-access-rotated",
          refresh_token: "oauth-refresh-rotated",
          expires_at: "2026-07-06T15:00:00+02:00",
        });
      }),
    });

    await expect(writer.refreshOpenAIOAuthCredential({
      workspaceId: "wksp_1",
      credential: expiredCredential(),
      abortSignal: NeverAbort,
    })).resolves.toMatchObject({ ok: true, credential: { expiresAt: "2026-07-06T13:00:00.000Z" } });

    expect(MAX_FOLLOWED_REDIRECTS).toBe(0);
    expect(requests).toHaveLength(1);
    expect(requests[0]?.url).toBe(OpenAICodexOAuthTokenEndpoint);
    expect(requests[0]?.init?.redirect).toBe("manual");
  });

  test("normalizes issuer timestamps and accepts only canonical persisted expiry", () => {
    expect(normalizeOpenAIOAuthExpiry("2024-02-29T23:30:00.123456-02:30")).toBe("2024-03-01T02:00:00.123Z");
    expect(parseCanonicalOpenAIOAuthExpiry("2024-02-29T23:30:00.123Z")).toBe(Date.parse("2024-02-29T23:30:00.123Z"));
    for (const invalid of [
      "2024-02-30T00:00:00.000Z",
      "2024-02-29",
      "2024-02-29 00:00:00.000Z",
      "2024-02-29T00:00:00Z",
      "2024-02-29T00:00:00.000+00:00",
    ]) {
      expect(parseCanonicalOpenAIOAuthExpiry(invalid), invalid).toBeUndefined();
    }
  });

  test.each([
    [300, "https://auth.openai.com/oauth/token"],
    [301, "https://auth.openai.com/oauth/token"],
    [302, "https://attacker.example/collect"],
    [303, "https://127.0.0.1/collect"],
    [304, undefined],
    [305, "http://auth.openai.com/oauth/token"],
    [306, "https://user:password@attacker.example/collect"],
    [307, "not a URL"],
    [308, "https://auth.openai.com/oauth/token"],
    [399, "https://auth.openai.com/oauth/token"],
  ] as const)("rejects redirect status %d after one credential-bearing issuer request", async (status, location) => {
    const refreshSentinel = "oauth-refresh-redirect-sentinel";
    const sql = await lockedCredentialSQL(await encryptedAuth({
      ...expiredStoredAuth(),
      refresh_token: refreshSentinel,
    }));
    let issuerRequests = 0;
    let bodyCancelCount = 0;
    let bodyReadCount = 0;
    let jsonParseCount = 0;
    const writer = new SQLOpenAIOAuthCredentialRefreshWriter({
      sql,
      masterKeyHex: MasterKeyHex,
      now: () => Now,
      fetch: pinnedOpenAIIssuerFetch(async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
        issuerRequests += 1;
        expect(String(input)).toBe(OpenAICodexOAuthTokenEndpoint);
        expect(init?.redirect).toBe("manual");
        expect(String(init?.body)).toContain(refreshSentinel);
        return {
          ok: false,
          status,
          headers: new Headers(location === undefined ? {} : { location }),
          body: status === 304 ? null : {
            cancel: async () => {
              bodyCancelCount += 1;
            },
          },
          text: async () => {
            bodyReadCount += 1;
            return refreshSentinel;
          },
          json: async () => {
            jsonParseCount += 1;
            return { access_token: refreshSentinel };
          },
        } as unknown as Response;
      }),
    });

    const result = await writer.refreshOpenAIOAuthCredential({
      workspaceId: "wksp_1",
      credential: { ...expiredCredential(), refreshToken: refreshSentinel },
      abortSignal: NeverAbort,
    });

    expect(result).toEqual({ ok: false, error: "credential_required" });
    expect(issuerRequests).toBe(1);
    expect(bodyCancelCount).toBe(status === 304 ? 0 : 1);
    expect(bodyReadCount).toBe(0);
    expect(jsonParseCount).toBe(0);
    expect(sql.updateCount).toBe(0);
    expect(JSON.stringify(result)).not.toContain(refreshSentinel);
  });

  test("classifies refresh due only inside the configured skew window", () => {
    expect(openAIOAuthCredentialRefreshDue(undefined, Now)).toBe(false);
    expect(openAIOAuthCredentialRefreshDue("", Now)).toBe(false);
    expect(openAIOAuthCredentialRefreshDue("not a date", Now)).toBe(false);
    expect(openAIOAuthCredentialRefreshDue(new Date(Now - 1).toISOString(), Now)).toBe(true);
    expect(openAIOAuthCredentialRefreshDue(new Date(Now + OpenAIOAuthRefreshSkewMs).toISOString(), Now)).toBe(true);
    expect(openAIOAuthCredentialRefreshDue(new Date(Now + OpenAIOAuthRefreshSkewMs + 1).toISOString(), Now)).toBe(false);
  });

  test("refreshes expired credentials under the SQL row lock and single-flights concurrent replicas", async () => {
    const sql = await lockedCredentialSQL(await encryptedAuth({
      type: "provider_oauth",
      provider_id: "openai",
      access_mode: "oauth",
      access_token: "oauth-access-old",
      refresh_token: "oauth-refresh-old",
      expires_at: "2000-01-01T00:00:00.000Z",
      account_id: "acct_1",
    }));
    const issuerRefreshTokens: string[] = [];
    const writer = new SQLOpenAIOAuthCredentialRefreshWriter({
      sql,
      masterKeyHex: MasterKeyHex,
      now: () => Now,
      fetch: pinnedOpenAIIssuerFetch(async (_input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
        const body = new URLSearchParams(String(init?.body));
        issuerRefreshTokens.push(body.get("refresh_token") ?? "");
        await new Promise((resolve) => setTimeout(resolve, 2));
        return Response.json({
          access_token: "oauth-access-rotated",
          refresh_token: "oauth-refresh-rotated",
          expires_in: 3600,
        });
      }),
    });

    const [first, second] = await Promise.all([
      writer.refreshOpenAIOAuthCredential({ workspaceId: "wksp_1", credential: expiredCredential(), abortSignal: NeverAbort }),
      writer.refreshOpenAIOAuthCredential({ workspaceId: "wksp_1", credential: expiredCredential(), abortSignal: NeverAbort }),
    ]);

    expect(first).toMatchObject({ ok: true, credential: { accessToken: "oauth-access-rotated", refreshToken: "oauth-refresh-rotated" } });
    expect(second).toMatchObject({ ok: true, credential: { accessToken: "oauth-access-rotated", refreshToken: "oauth-refresh-rotated" } });
    expect(issuerRefreshTokens).toEqual(["oauth-refresh-old"]);
    expect(sql.updateCount).toBe(1);
    expect(sql.row.expires_at).toBe("2026-07-06T13:00:00.000Z");
    await expect(decryptedRowAuth(sql)).resolves.toMatchObject({
      access_token: "oauth-access-rotated",
      refresh_token: "oauth-refresh-rotated",
      expires_at: "2026-07-06T13:00:00.000Z",
      account_id: "acct_1",
    });
  });

  test("returns a freshly rotated row on CAS mismatch without calling the issuer", async () => {
    const sql = await lockedCredentialSQL(await encryptedAuth(expiredStoredAuth()), {
      casWinnerAuth: {
        type: "provider_oauth",
        provider_id: "openai",
        access_mode: "oauth",
        access_token: "oauth-access-fresh",
        refresh_token: "oauth-refresh-fresh",
        expires_at: "2999-01-01T00:00:00.000Z",
        account_id: "acct_1",
      },
    });
    let issuerCalls = 0;
    const writer = new SQLOpenAIOAuthCredentialRefreshWriter({
      sql,
      masterKeyHex: MasterKeyHex,
      now: () => Now,
      fetch: pinnedOpenAIIssuerFetch(async () => {
        issuerCalls += 1;
        return Response.json({ access_token: "oauth-access-loser", refresh_token: "oauth-refresh-loser", expires_in: 3600 });
      }),
    });

    const result = await writer.refreshOpenAIOAuthCredential({ workspaceId: "wksp_1", credential: expiredCredential(), abortSignal: NeverAbort });

    expect(result).toMatchObject({
      ok: true,
      credential: {
        accessToken: "oauth-access-fresh",
        refreshToken: "oauth-refresh-fresh",
        expiresAt: "2999-01-01T00:00:00.000Z",
      },
    });
    expect(issuerCalls).toBe(1);
    expect(sql.queries.filter((query) => query.includes("SELECT id, auth_type"))).toHaveLength(2);
  });

  test("fails closed when the issuer refresh or write-back fails", async () => {
    const issuerFailureSQL = await lockedCredentialSQL(await encryptedAuth(expiredStoredAuth()));
    const issuerFailureWriter = new SQLOpenAIOAuthCredentialRefreshWriter({
      sql: issuerFailureSQL,
      masterKeyHex: MasterKeyHex,
      now: () => Now,
      fetch: pinnedOpenAIIssuerFetch(async () => new Response("bad", { status: 400 })),
    });

    await expect(issuerFailureWriter.refreshOpenAIOAuthCredential({
      workspaceId: "wksp_1",
      credential: expiredCredential(),
      abortSignal: NeverAbort,
    })).resolves.toEqual({ ok: false, error: "credential_required" });
    expect(issuerFailureSQL.updateCount).toBe(0);

    const writeFailureSQL = await lockedCredentialSQL(await encryptedAuth(expiredStoredAuth()), { failUpdate: true });
    const writeFailureWriter = new SQLOpenAIOAuthCredentialRefreshWriter({
      sql: writeFailureSQL,
      masterKeyHex: MasterKeyHex,
      now: () => Now,
      fetch: pinnedOpenAIIssuerFetch(async () => Response.json({
        access_token: "oauth-access-rotated",
        refresh_token: "oauth-refresh-rotated",
        expires_in: 3600,
      })),
    });

    await expect(writeFailureWriter.refreshOpenAIOAuthCredential({
      workspaceId: "wksp_1",
      credential: expiredCredential(),
      abortSignal: NeverAbort,
    })).resolves.toEqual({ ok: false, error: "credential_required" });
    expect(writeFailureSQL.updateCount).toBe(0);
  });

  test.each([
    ["caller cancellation", () => {
      const controller = new AbortController();
      queueMicrotask(() => controller.abort(new DOMException("cancelled", "AbortError")));
      return controller.signal;
    }],
    ["request deadline", () => AbortSignal.timeout(1)],
  ] as const)("bounds the issuer call by %s while holding the row lock", async (_name, signalFactory) => {
    const sql = await lockedCredentialSQL(await encryptedAuth(expiredStoredAuth()));
    let observedSignal: AbortSignal | undefined;
    const writer = new SQLOpenAIOAuthCredentialRefreshWriter({
      sql,
      masterKeyHex: MasterKeyHex,
      now: () => Now,
      fetch: pinnedOpenAIIssuerFetch(async (_input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
        observedSignal = init?.signal ?? undefined;
        if (init?.signal?.aborted) {
          throw init.signal.reason;
        }
        return await new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener("abort", () => reject(init.signal?.reason), { once: true });
        });
      }),
    });

    await expect(writer.refreshOpenAIOAuthCredential({
      workspaceId: "wksp_1",
      credential: expiredCredential(),
      abortSignal: signalFactory(),
    })).resolves.toEqual({ ok: false, error: "credential_required" });
    expect(observedSignal?.aborted).toBe(true);
    expect(sql.updateCount).toBe(0);
  });
});

interface StoredOAuthAuth {
  readonly type: "provider_oauth";
  readonly provider_id: "openai";
  readonly access_mode: "oauth";
  readonly access_token: string;
  readonly refresh_token: string;
  readonly expires_at: string;
  readonly account_id: string;
}

interface FakeCredentialRow {
  id: string;
  auth_type: "provider_oauth";
  provider_id: "openai";
  access_mode: "oauth";
  expires_at: string;
  encrypted_auth: Uint8Array;
  auth_public_json: string;
  archived_at: string | null;
  revoked_at: string | null;
}

type FakeCredentialSQL = GatewayCredentialSQL & {
  row: FakeCredentialRow;
  queries: string[];
  updateCount: number;
};

async function lockedCredentialSQL(encryptedAuth: Uint8Array, options: { readonly failUpdate?: boolean; readonly casWinnerAuth?: StoredOAuthAuth } = {}): Promise<FakeCredentialSQL> {
  const row: FakeCredentialRow = {
    id: "cred_openai_oauth",
    auth_type: "provider_oauth",
    provider_id: "openai",
    access_mode: "oauth",
    expires_at: "2000-01-01T00:00:00.000Z",
    encrypted_auth: encryptedAuth,
    auth_public_json: "{}",
    archived_at: null,
    revoked_at: null,
  };
  let lock = Promise.resolve();
  const sql = (async <T = unknown>(strings: TemplateStringsArray, ...values: unknown[]): Promise<T> => {
    const query = strings.join("?");
    sql.queries.push(query);
    if (query.includes("set_config")) {
      expect(values).toEqual(["wksp_1"]);
      return [] as T;
    }
    if (query.includes("SELECT id, auth_type")) {
      expect(values).toEqual(["wksp_1", "vlt_openai", "cred_openai_oauth"]);
      return [row] as T;
    }
    if (query.includes("UPDATE credentials")) {
      if (options.casWinnerAuth !== undefined) {
        row.encrypted_auth = await encryptedAuthForStored(options.casWinnerAuth);
        return [] as T;
      }
      if (options.failUpdate === true || values[11] !== row.encrypted_auth) {
        return [] as T;
      }
      row.encrypted_auth = values[0] as Uint8Array;
      row.auth_public_json = values[1] as string;
      row.provider_id = values[2] as "openai";
      row.access_mode = values[3] as "oauth";
      row.expires_at = values[4] as string;
      sql.updateCount += 1;
      return [{ id: row.id }] as T;
    }
    throw new Error(`unexpected SQL query: ${query}`);
  }) as unknown as FakeCredentialSQL;
  sql.row = row;
  sql.queries = [];
  sql.updateCount = 0;
  sql.begin = async <T>(fn: (tx: GatewayCredentialSQL) => Promise<T>): Promise<T> => {
    const prior = lock;
    let releaseLock!: () => void;
    lock = new Promise<void>((resolve) => {
      releaseLock = resolve;
    });
    await prior;
    try {
      return await fn(sql);
    } finally {
      releaseLock();
    }
  };
  return sql;
}

async function encryptedAuthForStored(auth: StoredOAuthAuth): Promise<Uint8Array> {
  return await encryptAES256GCM(new TextEncoder().encode(JSON.stringify(auth)), MasterKeyHex);
}

function expiredCredential(): ResolvedSessionOAuthCredential {
  return {
    source: "session",
    authType: "provider_oauth",
    providerId: "openai",
    supplyMode: "openai-chatgpt-oauth",
    vaultId: "vlt_openai",
    credentialId: "cred_openai_oauth",
    accessMode: "oauth",
    accessToken: "oauth-access-old",
    refreshToken: "oauth-refresh-old",
    expiresAt: "2000-01-01T00:00:00.000Z",
    accountId: "acct_1",
  };
}

function expiredStoredAuth(): StoredOAuthAuth {
  return {
    type: "provider_oauth",
    provider_id: "openai",
    access_mode: "oauth",
    access_token: "oauth-access-old",
    refresh_token: "oauth-refresh-old",
    expires_at: "2000-01-01T00:00:00.000Z",
    account_id: "acct_1",
  };
}

async function encryptedAuth(auth: StoredOAuthAuth): Promise<Uint8Array> {
  return await encryptAES256GCM(new TextEncoder().encode(JSON.stringify(auth)), MasterKeyHex, () => new Uint8Array(12).fill(3));
}

async function decryptedRowAuth(sql: FakeCredentialSQL): Promise<StoredOAuthAuth> {
  const plaintext = await decryptAES256GCM(sql.row.encrypted_auth, MasterKeyHex);
  return JSON.parse(new TextDecoder().decode(plaintext)) as StoredOAuthAuth;
}

function pinnedOpenAIIssuerFetch(handler: (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>) {
  return Object.assign(async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    expect(MAX_FOLLOWED_REDIRECTS).toBe(0);
    expect(String(input)).toBe(OpenAICodexOAuthTokenEndpoint);
    expect(init?.redirect).toBe("manual");
    return await handler(input, init);
  }, { preconnect: () => {} });
}
