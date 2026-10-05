import { describe, expect, test } from "bun:test";
import { readFile } from "node:fs/promises";
import { closeSync, fchmodSync, mkdtempSync, openSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ProviderFinishReason, ProviderContextRole } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { decryptAES256GCM } from "../../src/providers/crypto.js";
import { ProviderClientRegistry } from "../../src/providers/clients.js";
import { CachedPlatformCredentialPool, ProviderCredentialResolver, SQLGatewayCredentialStore } from "../../src/providers/credentials.js";
import { createPlatformKeyPhaseObserver, parsePlatformKeyArgs, runPlatformKeyCLI } from "../../../../scripts/platform-key.js";
import { validProviderRequest } from "./fixtures.js";
import type { FetchFunction } from "@ai-sdk/provider-utils";
import type { ProviderRequest, ProviderStreamEvent } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { PlatformKeyPhase, PlatformKeySQL } from "../../../../scripts/platform-key.js";

const MasterKeyHex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const DatabaseUrl = "postgres://ops.example/tetral";
const OpenAIFixtureUrl = new URL("../golden/fixtures/openai-gpt-5.5-responses-live-2026-07-06.sse", import.meta.url);

describe("Gateway platform-key ops CLI", () => {
  test.each(["disabled", "enabled", "throwing"] as const)("%s phase observer preserves encrypted insert and await/close order", async (mode) => {
    const phases: PlatformKeyPhase[] = [];
    const operations: string[] = [];
    const fakeSQL = createFakeSQL();
    const sql = Object.assign(async <T = unknown>(strings: TemplateStringsArray, ...values: unknown[]): Promise<T> => {
      operations.push("query");
      return await fakeSQL.sql<T>(strings, ...values);
    }, { close: async () => { operations.push("close"); await fakeSQL.sql.close(); } });
    const code = await runPlatformKeyCLI({
      argv: ["insert", "--provider", "anthropic", "--key-id", "pfk_observed", "--cache-scope", "scope"],
      env: { TETRAL_DATABASE_URL: DatabaseUrl, ENGINE_VAULT_KEY: MasterKeyHex },
      stdin: { async *[Symbol.asyncIterator]() { operations.push("stdin"); yield new TextEncoder().encode("observer-secret\n"); operations.push("stdin_eof"); } },
      sqlFactory: () => sql,
      randomBytes: () => new Uint8Array(12).fill(7),
      observePhase: mode === "disabled" ? undefined : (phase) => { phases.push(phase); operations.push(phase); if (mode === "throwing") throw new Error("observer failed"); },
    });
    expect(code).toBe(0);
    expect(operations).toEqual(mode === "disabled" ? ["stdin", "stdin_eof", "query", "close"] : ["cli_enter", "stdin_begin", "stdin", "stdin_eof", "stdin_complete", "query_begin", "query", "query_complete", "close_begin", "close", "close_complete"]);
    expect(fakeSQL.closed).toBe(true);
    expect(phases).toEqual(mode === "disabled" ? [] : ["cli_enter", "stdin_begin", "stdin_complete", "query_begin", "query_complete", "close_begin", "close_complete"]);
    expect(new TextDecoder().decode(await decryptAES256GCM(fakeSQL.queries[0]!.values[2] as Uint8Array, MasterKeyHex))).toBe("observer-secret");
  });

  test.each(["disabled", "enabled", "throwing"] as const)("%s observer preserves held query and close awaits", async (mode) => {
    const gate = () => {
      let resolve!: () => void;
      const promise = new Promise<void>((done) => { resolve = done; });
      return { promise, resolve };
    };
    const queryReached = gate(), queryRelease = gate(), closeReached = gate(), closeRelease = gate();
    const phases: PlatformKeyPhase[] = [];
    let settled = false;
    const sql = Object.assign(async <T = unknown>(): Promise<T> => {
      queryReached.resolve();
      await queryRelease.promise;
      return [{ key_id: "pfk_gated" }] as T;
    }, { close: async (options?: { readonly timeout?: number }) => {
      expect(options).toEqual({ timeout: 1 });
      closeReached.resolve();
      await closeRelease.promise;
    } });
    const pending = runPlatformKeyCLI({
      argv: ["insert", "--provider", "anthropic", "--key-id", "pfk_gated", "--cache-scope", "scope"],
      env: { TETRAL_DATABASE_URL: DatabaseUrl, ENGINE_VAULT_KEY: MasterKeyHex },
      stdin: textChunks("gated-secret"), sqlFactory: () => sql,
      observePhase: mode === "disabled" ? undefined : (phase) => { phases.push(phase); if (mode === "throwing") throw new Error("observer failed"); },
    }).then((code) => { settled = true; return code; });
    try {
      await queryReached.promise;
      expect(settled).toBe(false);
      expect(phases).toEqual(mode === "disabled" ? [] : ["cli_enter", "stdin_begin", "stdin_complete", "query_begin"]);
      queryRelease.resolve();
      await closeReached.promise;
      expect(settled).toBe(false);
      expect(phases).toEqual(mode === "disabled" ? [] : ["cli_enter", "stdin_begin", "stdin_complete", "query_begin", "query_complete", "close_begin"]);
      closeRelease.resolve();
      expect(await pending).toBe(0);
      expect(phases).toEqual(mode === "disabled" ? [] : ["cli_enter", "stdin_begin", "stdin_complete", "query_begin", "query_complete", "close_begin", "close_complete"]);
    } finally {
      queryRelease.resolve(); closeRelease.resolve();
      await pending;
    }
  });

  test("observer failures preserve query error handling and native close rejection", async () => {
    for (const rejectClose of [false, true]) {
      const phases: PlatformKeyPhase[] = [];
      let closes = 0;
      const nativeCloseError = new Error("native close failure");
      const sql = Object.assign(async <T = unknown>(): Promise<T> => { throw new Error("query failure"); }, {
        close: async () => { closes += 1; if (rejectClose) throw nativeCloseError; },
      });
      const stderr = createTextSink();
      const pending = runPlatformKeyCLI({
        argv: ["insert", "--provider", "openai", "--key-id", "pfk_error", "--cache-scope", "scope"],
        env: { TETRAL_DATABASE_URL: DatabaseUrl, ENGINE_VAULT_KEY: MasterKeyHex },
        stdin: textChunks("secret"), sqlFactory: () => sql, stderr: stderr.writer,
        observePhase: (phase) => { phases.push(phase); throw new Error("observer failed"); },
      });
      if (rejectClose) await expect(pending).rejects.toBe(nativeCloseError);
      else expect(await pending).toBe(1);
      expect(closes).toBe(1);
      expect(stderr.text()).toContain("query failure");
      expect(stderr.text()).not.toContain("observer failed");
      expect(phases).toEqual(["cli_enter", "stdin_begin", "stdin_complete", "query_begin", "body_error", "close_begin", ...(rejectClose ? [] : ["close_complete" as const])]);
    }
  });

  test("explicit private descriptor bounds phase records and fails open after closure", () => {
    const directory = mkdtempSync(join(tmpdir(), "platform-key-observer-"));
    const path = join(directory, "phases");
    const fd = openSync(path, "wx", 0o600);
    let closed = false;
    try {
      expect(createPlatformKeyPhaseObserver({})).toBeUndefined();
      for (const value of ["", "0", "1", "2", "03", "3x", "1025"]) {
        expect(createPlatformKeyPhaseObserver({ TETRAL_PLATFORM_KEY_DIAGNOSTIC_FD: value })).toBeUndefined();
      }
      const observer = createPlatformKeyPhaseObserver({ TETRAL_PLATFORM_KEY_DIAGNOSTIC_FD: String(fd) });
      expect(observer).toBeDefined();
      for (let index = 0; index < 12; index++) observer!("cli_enter");
      const records = readFileSync(path, "utf8").trim().split("\n").map((line) => JSON.parse(line) as Record<string, unknown>);
      expect(records).toHaveLength(9);
      expect(readFileSync(path).byteLength).toBeLessThanOrEqual(4096);
      let previous = 0;
      for (const record of records) {
        expect(Object.keys(record)).toEqual(["phase", "elapsed_ms"]);
        expect(record.phase).toBe("cli_enter");
        expect(Number.isFinite(record.elapsed_ms)).toBe(true);
        expect(record.elapsed_ms as number).toBeGreaterThanOrEqual(previous);
        previous = record.elapsed_ms as number;
      }
      const failing = createPlatformKeyPhaseObserver({ TETRAL_PLATFORM_KEY_DIAGNOSTIC_FD: String(fd) })!;
      closeSync(fd); closed = true;
      expect(() => failing("stdin_begin")).not.toThrow();
      expect(() => failing("query_begin")).not.toThrow();
      expect(readFileSync(path, "utf8").trim().split("\n")).toHaveLength(9);
      expect(createPlatformKeyPhaseObserver({ TETRAL_PLATFORM_KEY_DIAGNOSTIC_FD: String(fd) })).toBeUndefined();
    } finally {
      if (!closed) closeSync(fd);
      rmSync(directory, { recursive: true, force: true });
    }
  });

  test("nonprivate files and out-of-contract phase values disable observations", () => {
    const directory = mkdtempSync(join(tmpdir(), "platform-key-observer-"));
    const path = join(directory, "phases");
    const fd = openSync(path, "wx", 0o644);
    fchmodSync(fd, 0o644);
    try {
      expect(createPlatformKeyPhaseObserver({ TETRAL_PLATFORM_KEY_DIAGNOSTIC_FD: String(fd) })).toBeUndefined();
    } finally { closeSync(fd); }
    const privateFD = openSync(join(directory, "private"), "wx", 0o600);
    try {
      const observer = createPlatformKeyPhaseObserver({ TETRAL_PLATFORM_KEY_DIAGNOSTIC_FD: String(privateFD) })!;
      observer("secret-sentinel" as PlatformKeyPhase);
      observer("cli_enter");
      expect(readFileSync(join(directory, "private")).byteLength).toBe(0);
    } finally { closeSync(privateFD); rmSync(directory, { recursive: true, force: true }); }
  });

  test.each([
    ["retired master key flag", ["insert", "--provider", "openai", "--key-id", "pfk_rejected", "--cache-scope", "scope", "--master-key-hex", "argv-master-key-sentinel"]],
    ["retired database URL flag", ["disable", "--key-id", "pfk_rejected", "--database-url", "argv-database-secret-sentinel"]],
    ["unknown flag", ["disable", "--key-id", "pfk_rejected", "--provider-secret", "unknown-flag-secret-sentinel"]],
    ["duplicate flag", ["disable", "--key-id", "pfk_rejected", "--key-id", "duplicate-value-secret-sentinel"]],
    ["help with retired secret flag", ["--help", "--master-key-hex", "help-retired-secret-sentinel"]],
    ["help command with retired secret flag", ["help", "--database-url", "help-command-retired-secret-sentinel"]],
    ["short help with retired secret flag", ["-h", "--database-url", "short-help-retired-secret-sentinel"]],
    ["help with unknown trailing flag", ["--help", "--topic", "help-unknown-secret-sentinel"]],
    ["help with duplicate trailing flag", ["--help", "--topic", "one", "--topic", "help-duplicate-secret-sentinel"]],
    ["help with positional trailing value", ["--help", "help-positional-secret-sentinel"]],
  ] as const)("rejects %s before SQL or stdin and emits no supplied secret", async (_name, argv) => {
    const stdout = createTextSink();
    const stderr = createTextSink();
    let sqlFactoryCalls = 0;
    let stdinIterations = 0;
    const stdin = {
      async *[Symbol.asyncIterator](): AsyncIterator<Uint8Array> {
        stdinIterations += 1;
        yield new TextEncoder().encode("provider-stdin-secret-sentinel");
      },
    };

    const code = await runPlatformKeyCLI({
      argv,
      env: {
        TETRAL_DATABASE_URL: DatabaseUrl,
        ENGINE_VAULT_KEY: MasterKeyHex,
      },
      stdin,
      stdout: stdout.writer,
      stderr: stderr.writer,
      sqlFactory: () => {
        sqlFactoryCalls += 1;
        return createFakeSQL().sql;
      },
    });

    expect(code).toBe(1);
    expect(sqlFactoryCalls).toBe(0);
    expect(stdinIterations).toBe(0);
    for (const secret of [argv.at(-1) ?? "", "provider-stdin-secret-sentinel", DatabaseUrl, MasterKeyHex]) {
      expect(stdout.text()).not.toContain(secret);
      expect(stderr.text()).not.toContain(secret);
    }
  });

  test("help and supported env-plus-stdin argv contain no credential material", async () => {
    const help = createTextSink();
    expect(await runPlatformKeyCLI({ argv: ["--help"], stdout: help.writer })).toBe(0);
    expect(help.text()).not.toContain("--master-key-hex");
    expect(help.text()).not.toContain("--database-url");

    const supportedArgv = ["insert", "--provider", "openai", "--key-id", "pfk_secret_free_argv", "--cache-scope", "scope"];
    const secretCorpus = [DatabaseUrl, MasterKeyHex, "supported-provider-key-sentinel"];
    for (const secret of secretCorpus) {
      expect(JSON.stringify(supportedArgv)).not.toContain(secret);
      expect(help.text()).not.toContain(secret);
    }
  });

  test("forced SQL failures scrub environment secrets from output", async () => {
    const stdout = createTextSink();
    const stderr = createTextSink();
    const code = await runPlatformKeyCLI({
      argv: ["disable", "--key-id", "pfk_failure"],
      env: { TETRAL_DATABASE_URL: DatabaseUrl, ENGINE_VAULT_KEY: MasterKeyHex },
      stdout: stdout.writer,
      stderr: stderr.writer,
      sqlFactory: () => {
        throw new Error(`database connection failed for ${DatabaseUrl} using ${MasterKeyHex}`);
      },
    });

    expect(code).toBe(1);
    for (const secret of [DatabaseUrl, MasterKeyHex]) {
      expect(stdout.text()).not.toContain(secret);
      expect(stderr.text()).not.toContain(secret);
    }
  });

  test("forced encryption failures cannot echo the stdin provider key", async () => {
    const providerKey = "provider-encryption-failure-secret-sentinel";
    const stdout = createTextSink();
    const stderr = createTextSink();
    const code = await runPlatformKeyCLI({
      argv: ["insert", "--provider", "openai", "--key-id", "pfk_failure", "--cache-scope", "scope"],
      env: { TETRAL_DATABASE_URL: DatabaseUrl, ENGINE_VAULT_KEY: MasterKeyHex },
      stdin: textChunks(providerKey),
      stdout: stdout.writer,
      stderr: stderr.writer,
      sqlFactory: () => createFakeSQL().sql,
      randomBytes: () => {
        throw new Error(`encryption failed for ${providerKey}`);
      },
    });

    expect(code).toBe(1);
    expect(stdout.text()).not.toContain(providerKey);
    expect(stderr.text()).not.toContain(providerKey);
  });

  test("insert reads plaintext only from stdin, encrypts with aesgcm.go framing, and upserts an active row", async () => {
    const fakeSQL = createFakeSQL();
    const stdout = createTextSink();
    const stderr = createTextSink();
    const argv = [
      "insert",
      "--provider",
      "anthropic",
      "--key-id",
      "pfk_anthropic_ops",
      "--cache-scope",
      "anthropic-workspace-01",
      "--weight",
      "2",
      "--priority",
      "1",
    ];

    const code = await runPlatformKeyCLI({
      argv,
      env: {
        TETRAL_DATABASE_URL: DatabaseUrl,
        ENGINE_VAULT_KEY: MasterKeyHex,
      },
      stdin: textChunks("fixture-provider-secret\n"),
      stdout: stdout.writer,
      stderr: stderr.writer,
      sqlFactory: (databaseUrl) => {
        expect(databaseUrl).toBe(DatabaseUrl);
        return fakeSQL.sql;
      },
      randomBytes: () => new Uint8Array(12).fill(9),
    });

    expect(code).toBe(0);
    expect(stdout.text()).toBe("upserted pfk_anthropic_ops for anthropic\n");
    expect(stderr.text()).toBe("");
    expect(fakeSQL.closed).toBe(true);
    expect(fakeSQL.queries).toHaveLength(1);

    const query = fakeSQL.queries[0]!;
    expect(query.text).toContain("INSERT INTO platform_provider_keys");
    expect(query.text).toContain("ON CONFLICT (key_id) DO UPDATE");
    expect(query.text).toContain("RETURNING key_id");
    expect(query.values[0]).toBe("pfk_anthropic_ops");
    expect(query.values[1]).toBe("anthropic");
    expect(query.values[3]).toBe(2);
    expect(query.values[4]).toBe(1);
    expect(query.values[5]).toBe("anthropic-workspace-01");

    const encrypted = query.values[2];
    expect(encrypted).toBeInstanceOf(Uint8Array);
    const plaintext = await decryptAES256GCM(encrypted as Uint8Array, MasterKeyHex);
    expect(new TextDecoder().decode(plaintext)).toBe("fixture-provider-secret");
    expect(JSON.stringify(argv)).not.toContain("fixture-provider-secret");
    expect(JSON.stringify(query.values)).not.toContain("fixture-provider-secret");
  });

  test("disable and enable update status without reading stdin", async () => {
    const disableSQL = createFakeSQL();
    const disabled = await runPlatformKeyCLI({
      argv: ["disable", "--key-id", "pfk_anthropic_ops", "--reason", "leak_response"],
      env: { TETRAL_DATABASE_URL: DatabaseUrl },
      sqlFactory: () => disableSQL.sql,
    });

    expect(disabled).toBe(0);
    expect(disableSQL.queries).toHaveLength(1);
    expect(disableSQL.queries[0]!.text).toContain("SET status = 'disabled'");
    expect(disableSQL.queries[0]!.values).toEqual(["leak_response", "pfk_anthropic_ops"]);
    expect(disableSQL.closed).toBe(true);

    const enableSQL = createFakeSQL();
    const enabled = await runPlatformKeyCLI({
      argv: ["enable", "--key-id", "pfk_anthropic_ops"],
      env: { TETRAL_DATABASE_URL: DatabaseUrl },
      sqlFactory: () => enableSQL.sql,
    });

    expect(enabled).toBe(0);
    expect(enableSQL.queries).toHaveLength(1);
    expect(enableSQL.queries[0]!.text).toContain("SET status = 'active'");
    expect(enableSQL.queries[0]!.text).toContain("disabled_reason = NULL");
    expect(enableSQL.queries[0]!.values).toEqual(["pfk_anthropic_ops"]);
    expect(enableSQL.closed).toBe(true);
  });

  test("missing platform key rows fail closed for status updates", async () => {
    const fakeSQL = createFakeSQL(() => []);
    const stderr = createTextSink();

    const code = await runPlatformKeyCLI({
      argv: ["disable", "--key-id", "pfk_missing"],
      env: { TETRAL_DATABASE_URL: DatabaseUrl },
      stderr: stderr.writer,
      sqlFactory: () => fakeSQL.sql,
    });

    expect(code).toBe(1);
    expect(stderr.text()).toContain("platform key row not found: pfk_missing");
    expect(fakeSQL.closed).toBe(true);
  });

  test("argument validation pins platform-hosted providers and safe key ids", () => {
    expect(parsePlatformKeyArgs([
      "insert",
      "--provider",
      "openai",
      "--key-id",
      "pfk_openai_ops",
      "--cache-scope",
      "openai-org",
    ], {
      TETRAL_DATABASE_URL: DatabaseUrl,
      ENGINE_VAULT_KEY: MasterKeyHex,
    })).toMatchObject({
      command: "insert",
      providerId: "openai",
      keyId: "pfk_openai_ops",
      cacheScope: "openai-org",
      weight: 1,
      priority: 0,
      databaseUrl: DatabaseUrl,
      masterKeyHex: MasterKeyHex,
    });

    expect(() => parsePlatformKeyArgs([
      "insert",
      "--provider",
      "moonshotai",
      "--key-id",
      "pfk_kimi",
      "--cache-scope",
      "scope",
    ], {
      TETRAL_DATABASE_URL: DatabaseUrl,
      ENGINE_VAULT_KEY: MasterKeyHex,
    })).toThrow("provider must be one of anthropic, openai, or deepseek");
    expect(() => parsePlatformKeyArgs(["disable", "--key-id", "session_key"], { TETRAL_DATABASE_URL: DatabaseUrl })).toThrow("key-id must start with pfk_");
  });

  test("T-POOL-10 CLI-written platform key decrypts through the gateway and serves a golden turn", async () => {
    const cliSQL = createFakeSQL();
    const code = await runPlatformKeyCLI({
      argv: [
        "insert",
        "--provider",
        "openai",
        "--key-id",
        "pfk_openai_cli_golden",
        "--cache-scope",
        "openai-org-01",
        "--weight",
        "4",
      ],
      env: {
        TETRAL_DATABASE_URL: DatabaseUrl,
        ENGINE_VAULT_KEY: MasterKeyHex,
      },
      stdin: textChunks("fixture-platform-openai-key\n"),
      sqlFactory: () => cliSQL.sql,
      randomBytes: () => new Uint8Array(12).fill(4),
    });
    expect(code).toBe(0);

    const row = platformRowFromInsert(cliSQL.queries[0]!);
    const store = new SQLGatewayCredentialStore(selectOnlySQL(row));
    const platformPool = new CachedPlatformCredentialPool({
      store,
      masterKeyHex: MasterKeyHex,
      poolOptions: { random: () => 0 },
    });
    const resolver = new ProviderCredentialResolver({
      store,
      platformPool,
      masterKeyHex: MasterKeyHex,
    });
    const request = openAIGoldenRequest();
    const resolved = await resolver.resolve(request);

    expect(resolved).toMatchObject({
      ok: true,
      credential: {
        source: "platform",
        providerId: "openai",
        platformKey: {
          keyId: "pfk_openai_cli_golden",
          cacheScope: "openai-org-01",
          weight: 4,
        },
      },
    });
    if (!resolved.ok) {
      throw new Error("expected platform credential");
    }

    const fixture = await readFile(OpenAIFixtureUrl, "utf8");
    const mock = createMockProviderServer(fixture);
    const registry = new ProviderClientRegistry({ fetch: mock.fetch });
    try {
      const events = await collectEvents(registry.stream({
        request,
        credential: resolved.credential,
      }));

      expect(mock.requests).toHaveLength(1);
      expect(mock.requests[0]?.headers.authorization).toBe("Bearer fixture-platform-openai-key");
      expect(mock.requests[0]?.body).toMatchObject({
        model: "gpt-5.5",
        stream: true,
        store: false,
      });
      expect(events.at(-1)?.finish).toMatchObject({
        reason: ProviderFinishReason.PROVIDER_FINISH_REASON_TOOL_CALLS,
        usage: {
          inputCacheReadTokens: 0,
          inputTotalTokens: 98,
          outputReasoningTokens: 1034,
          outputTotalTokens: 1062,
          totalTokens: 1160,
        },
        metadataJson: JSON.stringify({ credential_source: "platform" }),
      });
    } finally {
      await mock.close();
    }
  });
});

interface CapturedSQLQuery {
  readonly text: string;
  readonly values: readonly unknown[];
}

interface PlatformProviderKeySQLRow {
  readonly key_id: string;
  readonly provider_id: "anthropic" | "openai" | "deepseek";
  readonly encrypted_key: Uint8Array;
  readonly weight: number;
  readonly priority: number;
  readonly cache_scope: string;
  readonly status: "active";
  readonly disabled_reason: null;
  readonly updated_at: string;
}

function createFakeSQL(rowsForQuery: (query: CapturedSQLQuery) => readonly { readonly key_id: string }[] = defaultRowsForQuery): {
  readonly sql: PlatformKeySQL & { readonly close: () => Promise<void> };
  readonly queries: CapturedSQLQuery[];
  readonly closed: boolean;
} {
  const queries: CapturedSQLQuery[] = [];
  let closed = false;
  const sql = Object.assign(async <T = unknown>(strings: TemplateStringsArray, ...values: unknown[]): Promise<T> => {
    const query = { text: strings.join("$"), values };
    queries.push(query);
    return rowsForQuery(query) as T;
  }, {
    close: async () => {
      closed = true;
    },
  }) satisfies PlatformKeySQL & { readonly close: () => Promise<void> };
  return {
    sql,
    queries,
    get closed() {
      return closed;
    },
  };
}

function defaultRowsForQuery(query: CapturedSQLQuery): readonly { readonly key_id: string }[] {
  const keyId = query.values.find((value): value is string => typeof value === "string" && value.startsWith("pfk_"));
  return keyId === undefined ? [] : [{ key_id: keyId }];
}

function platformRowFromInsert(query: CapturedSQLQuery): PlatformProviderKeySQLRow {
  return {
    key_id: query.values[0] as string,
    provider_id: query.values[1] as PlatformProviderKeySQLRow["provider_id"],
    encrypted_key: query.values[2] as Uint8Array,
    weight: query.values[3] as number,
    priority: query.values[4] as number,
    cache_scope: query.values[5] as string,
    status: "active",
    disabled_reason: null,
    updated_at: "2026-07-06T00:00:00Z",
  };
}

function selectOnlySQL(row: PlatformProviderKeySQLRow): PlatformKeySQL {
  return async <T = unknown>(strings: TemplateStringsArray): Promise<T> => {
    const query = strings.join("$");
    if (query.includes("session_provider_auth")) {
      return [] as T;
    }
    if (query.includes("platform_provider_keys")) {
      return [row] as T;
    }
    throw new Error(`unexpected query: ${query}`);
  };
}

interface CapturedProviderRequest {
  readonly method: string;
  readonly pathname: string;
  readonly headers: Record<string, string>;
  readonly body: Record<string, unknown>;
}

function createMockProviderServer(fixture: string): {
  readonly requests: CapturedProviderRequest[];
  readonly fetch: FetchFunction;
  readonly close: () => Promise<void>;
} {
  const requests: CapturedProviderRequest[] = [];
  let server: ReturnType<typeof Bun.serve>;
  server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      const url = new URL(request.url);
      requests.push({
        method: request.method,
        pathname: url.pathname,
        headers: Object.fromEntries(Array.from(request.headers.entries()).map(([key, value]) => [key.toLowerCase(), value])),
        body: await request.json() as Record<string, unknown>,
      });
      return new Response(sseFixtureResponseBody(fixture), {
        headers: {
          "content-type": "text/event-stream",
          "cache-control": "no-cache",
        },
      });
    },
  });
  return {
    requests,
    fetch: Object.assign(async (input: Parameters<FetchFunction>[0], init?: Parameters<FetchFunction>[1]) => {
      const original = new Request(input, init);
      const originalUrl = new URL(original.url);
      const body = original.method === "GET" || original.method === "HEAD" ? undefined : await original.text();
      const requestInit: RequestInit = {
        method: original.method,
        headers: original.headers,
        signal: original.signal,
      };
      if (body !== undefined) {
        requestInit.body = body;
      }
      return fetch(new URL(`${originalUrl.pathname}${originalUrl.search}`, server.url), requestInit);
    }, {
      preconnect: () => {},
    }) satisfies FetchFunction,
    close: async () => {
      await server.stop(true);
    },
  };
}

function sseFixtureResponseBody(fixture: string): string {
  return fixture.endsWith("\n\n") ? fixture : `${fixture}\n`;
}

function openAIGoldenRequest(): ProviderRequest {
  return validProviderRequest({
    requestId: "req_pool_openai_1",
    modelRequestId: "mreq_pool_openai_1",
    workspaceId: "wksp_pool",
    sessionId: "sesn_pool_openai",
    model: { providerId: "openai", modelId: "gpt-5.5", variant: "xhigh" },
    context: [
      {
        role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
        content: [{
          text: { text: "Say ok and call Search once." },
        }],
      },
    ],
    tools: [{
      name: "Search",
      description: "Search.",
      function: { inputSchemaJson: JSON.stringify({
        type: "object",
        properties: { query: { type: "string" } },
        required: ["query"],
        additionalProperties: false,
      }), outputSchemaJson: undefined },
    }],
    attachments: [],
    limits: { maxOutputTokens: 1024, timeoutMs: 60_000 },
  });
}

async function collectEvents<T>(events: AsyncIterable<T>): Promise<readonly T[]> {
  const output: T[] = [];
  for await (const event of events) {
    output.push(event);
  }
  return output;
}

function createTextSink(): {
  readonly writer: { readonly write: (chunk: Uint8Array) => void };
  readonly text: () => string;
} {
  let output = "";
  return {
    writer: {
      write(chunk: Uint8Array): void {
        output += new TextDecoder().decode(chunk);
      },
    },
    text: () => output,
  };
}

async function* textChunks(...chunks: readonly string[]): AsyncIterable<Uint8Array> {
  for (const chunk of chunks) {
    yield new TextEncoder().encode(chunk);
  }
}
