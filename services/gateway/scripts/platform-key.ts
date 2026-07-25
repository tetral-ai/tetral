import { encryptAES256GCM } from "../packages/provider-gateway/src/providers/crypto.js";

type PlatformProviderId = "anthropic" | "openai" | "deepseek";
type PlatformKeyStatusCommand = "disable" | "enable";

export interface PlatformKeySQL {
  <T = unknown>(strings: TemplateStringsArray, ...values: unknown[]): PromiseLike<T>;
}

interface TextByteWriter {
  readonly write: (chunk: Uint8Array) => unknown;
}

export interface PlatformKeyCLIOptions {
  readonly argv: readonly string[];
  readonly env?: Record<string, string | undefined> | undefined;
  readonly stdin?: AsyncIterable<Uint8Array> | undefined;
  readonly stdout?: TextByteWriter | undefined;
  readonly stderr?: TextByteWriter | undefined;
  readonly sqlFactory?: ((databaseUrl: string) => PlatformKeySQL & { readonly close?: (options?: { readonly timeout?: number }) => Promise<void> }) | undefined;
  readonly randomBytes?: ((length: number) => Uint8Array) | undefined;
}

interface ParsedInsertCommand {
  readonly command: "insert";
  readonly providerId: PlatformProviderId;
  readonly keyId: string;
  readonly cacheScope: string;
  readonly weight: number;
  readonly priority: number;
  readonly databaseUrl: string;
  readonly masterKeyHex: string;
}

interface ParsedStatusCommand {
  readonly command: PlatformKeyStatusCommand;
  readonly keyId: string;
  readonly disabledReason?: string | undefined;
  readonly databaseUrl: string;
}

type ParsedCommand = ParsedInsertCommand | ParsedStatusCommand | { readonly command: "help" };

class PlatformKeyCLIError extends Error {}

export async function runPlatformKeyCLI(options: PlatformKeyCLIOptions): Promise<number> {
  let sql: (PlatformKeySQL & { readonly close?: (options?: { readonly timeout?: number }) => Promise<void> }) | undefined;
  const env = options.env ?? process.env;
  try {
    const parsed = parsePlatformKeyArgs(options.argv, env);
    if (parsed.command === "help") {
      await writeText(options.stdout, usageText());
      return 0;
    }
    sql = options.sqlFactory?.(parsed.databaseUrl) ?? new Bun.SQL(parsed.databaseUrl);
    if (parsed.command === "insert") {
      const plaintext = await readPlaintextKey(options.stdin ?? Bun.stdin.stream());
      if (plaintext.byteLength === 0) {
        throw new PlatformKeyCLIError("platform key stdin is empty");
      }
      const encrypted = await encryptAES256GCM(plaintext, parsed.masterKeyHex, options.randomBytes);
      const rows = await sql<readonly { readonly key_id: string }[]>`
        INSERT INTO platform_provider_keys (
          key_id,
          provider_id,
          encrypted_key,
          weight,
          priority,
          cache_scope,
          status,
          disabled_reason,
          updated_at
        )
        VALUES (
          ${parsed.keyId},
          ${parsed.providerId},
          ${encrypted},
          ${parsed.weight},
          ${parsed.priority},
          ${parsed.cacheScope},
          'active',
          NULL,
          now()::text
        )
        ON CONFLICT (key_id) DO UPDATE SET
          provider_id = EXCLUDED.provider_id,
          encrypted_key = EXCLUDED.encrypted_key,
          weight = EXCLUDED.weight,
          priority = EXCLUDED.priority,
          cache_scope = EXCLUDED.cache_scope,
          status = 'active',
          disabled_reason = NULL,
          updated_at = now()::text
        RETURNING key_id
      `;
      assertOneRow(rows, parsed.keyId);
      await writeText(options.stdout, `upserted ${parsed.keyId} for ${parsed.providerId}\n`);
      return 0;
    }
    if (parsed.command === "disable") {
      await updatePlatformKeyStatus(sql, parsed);
      await writeText(options.stdout, `disabled ${parsed.keyId}\n`);
      return 0;
    }
    await updatePlatformKeyStatus(sql, parsed);
    await writeText(options.stdout, `enabled ${parsed.keyId}\n`);
    return 0;
  } catch (error) {
    await writeText(options.stderr, `${safeCLIErrorMessage(error, env)}\n`);
    return 1;
  } finally {
    await sql?.close?.({ timeout: 1 });
  }
}

export function parsePlatformKeyArgs(argv: readonly string[], env: Record<string, string | undefined>): ParsedCommand {
  const [command, ...args] = argv;
  if (command === undefined) {
    return { command: "help" };
  }
  const flags = parseFlags(args);
  rejectSecretBearingFlags(flags);
  if (command === "help" || command === "--help" || command === "-h") {
    validateAllowedFlags(flags, new Set());
    return { command: "help" };
  }
  switch (command) {
    case "insert":
    case "upsert": {
      validateAllowedFlags(flags, new Set(["provider", "key-id", "cache-scope", "weight", "priority"]));
      return {
        command: "insert",
        providerId: parseProviderId(requireFlag(flags, "provider")),
        keyId: parseKeyId(requireFlag(flags, "key-id")),
        cacheScope: nonEmpty(requireFlag(flags, "cache-scope"), "cache-scope"),
        weight: parseNonNegativeInteger(optionalFlag(flags, "weight") ?? "1", "weight"),
        priority: parseNonNegativeInteger(optionalFlag(flags, "priority") ?? "0", "priority"),
        databaseUrl: nonEmpty(env.TETRAL_DATABASE_URL ?? "", "TETRAL_DATABASE_URL"),
        masterKeyHex: parseMasterKeyHex(nonEmpty(env.ENGINE_VAULT_KEY ?? "", "ENGINE_VAULT_KEY")),
      };
    }
    case "disable": {
      validateAllowedFlags(flags, new Set(["key-id", "reason"]));
      return {
        command,
        keyId: parseKeyId(requireFlag(flags, "key-id")),
        disabledReason: optionalFlag(flags, "reason") ?? "operator_disabled",
        databaseUrl: nonEmpty(env.TETRAL_DATABASE_URL ?? "", "TETRAL_DATABASE_URL"),
      };
    }
    case "enable": {
      validateAllowedFlags(flags, new Set(["key-id"]));
      return {
        command,
        keyId: parseKeyId(requireFlag(flags, "key-id")),
        databaseUrl: nonEmpty(env.TETRAL_DATABASE_URL ?? "", "TETRAL_DATABASE_URL"),
      };
    }
    default:
      throw new PlatformKeyCLIError(`unknown platform-key command: ${command}`);
  }
}

async function updatePlatformKeyStatus(sql: PlatformKeySQL, command: ParsedStatusCommand): Promise<void> {
  if (command.command === "disable") {
    const rows = await sql<readonly { readonly key_id: string }[]>`
      UPDATE platform_provider_keys
         SET status = 'disabled',
             disabled_reason = ${command.disabledReason ?? "operator_disabled"},
             updated_at = now()::text
       WHERE key_id = ${command.keyId}
       RETURNING key_id
    `;
    assertOneRow(rows, command.keyId);
    return;
  }
  const rows = await sql<readonly { readonly key_id: string }[]>`
    UPDATE platform_provider_keys
       SET status = 'active',
           disabled_reason = NULL,
           updated_at = now()::text
     WHERE key_id = ${command.keyId}
     RETURNING key_id
  `;
  assertOneRow(rows, command.keyId);
}

async function readPlaintextKey(stdin: AsyncIterable<Uint8Array>): Promise<Uint8Array> {
  const chunks: Uint8Array[] = [];
  for await (const chunk of stdin) {
    chunks.push(chunk);
  }
  const total = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0);
  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return stripOneTrailingNewline(bytes);
}

function stripOneTrailingNewline(bytes: Uint8Array): Uint8Array {
  if (bytes.at(-1) === 0x0a) {
    const end = bytes.at(-2) === 0x0d ? bytes.byteLength - 2 : bytes.byteLength - 1;
    return bytes.slice(0, end);
  }
  return bytes;
}

function parseFlags(args: readonly string[]): Map<string, string> {
  const flags = new Map<string, string>();
  for (let index = 0; index < args.length; index += 1) {
    const key = args[index];
    const value = args[index + 1];
    if (key === undefined || !key.startsWith("--") || value === undefined || value.startsWith("--")) {
      throw new PlatformKeyCLIError("platform-key flags must be --name value pairs");
    }
    const name = key.slice(2);
    if (flags.has(name)) {
      throw new PlatformKeyCLIError(`duplicate platform-key flag: --${name}`);
    }
    flags.set(name, value);
    index += 1;
  }
  return flags;
}

function rejectSecretBearingFlags(flags: ReadonlyMap<string, string>): void {
  for (const retired of ["master-key-hex", "database-url"]) {
    if (flags.has(retired)) {
      throw new PlatformKeyCLIError(`secret-bearing argv flag is not supported: --${retired}`);
    }
  }
}

function validateAllowedFlags(flags: ReadonlyMap<string, string>, allowed: ReadonlySet<string>): void {
  for (const name of flags.keys()) {
    if (!allowed.has(name)) {
      throw new PlatformKeyCLIError(`unknown platform-key flag: --${name}`);
    }
  }
}

function requireFlag(flags: ReadonlyMap<string, string>, name: string, fallback?: string | undefined): string {
  return nonEmpty(optionalFlag(flags, name) ?? fallback ?? "", name);
}

function optionalFlag(flags: ReadonlyMap<string, string>, name: string): string | undefined {
  return flags.get(name);
}

function parseProviderId(value: string): PlatformProviderId {
  if (value === "anthropic" || value === "openai" || value === "deepseek") {
    return value;
  }
  throw new PlatformKeyCLIError("provider must be one of anthropic, openai, or deepseek");
}

function parseKeyId(value: string): string {
  const keyId = nonEmpty(value, "key-id");
  if (!/^pfk_[A-Za-z0-9_-]+$/.test(keyId)) {
    throw new PlatformKeyCLIError("key-id must start with pfk_ and contain only alphanumeric, underscore, or dash characters");
  }
  return keyId;
}

function parseMasterKeyHex(value: string): string {
  if (!/^[0-9a-fA-F]{64}$/.test(value)) {
    throw new PlatformKeyCLIError("master key must be a 64-character hex AES-256 key");
  }
  return value;
}

function parseNonNegativeInteger(value: string, name: string): number {
  if (!/^(0|[1-9][0-9]*)$/.test(value)) {
    throw new PlatformKeyCLIError(`${name} must be a non-negative integer`);
  }
  const parsed = Number.parseInt(value, 10);
  if (!Number.isSafeInteger(parsed)) {
    throw new PlatformKeyCLIError(`${name} is too large`);
  }
  return parsed;
}

function nonEmpty(value: string, name: string): string {
  if (value.length === 0) {
    throw new PlatformKeyCLIError(`${name} is required`);
  }
  return value;
}

async function writeText(writer: TextByteWriter | undefined, text: string): Promise<void> {
  if (writer === undefined) {
    return;
  }
  await writer.write(new TextEncoder().encode(text));
}

function assertOneRow(rows: readonly { readonly key_id: string }[], keyId: string): void {
  if (rows.length !== 1 || rows[0]?.key_id !== keyId) {
    throw new PlatformKeyCLIError(`platform key row not found: ${keyId}`);
  }
}

function usageText(): string {
  return [
    "Usage:",
    "  bun scripts/platform-key.ts insert --provider anthropic|openai|deepseek --key-id pfk_<id> --cache-scope <scope> [--weight 1] [--priority 0]",
    "  bun scripts/platform-key.ts disable --key-id pfk_<id> [--reason operator_disabled]",
    "  bun scripts/platform-key.ts enable --key-id pfk_<id>",
    "",
    "Config:",
    "  TETRAL_DATABASE_URL supplies the operator database connection.",
    "  ENGINE_VAULT_KEY supplies the 64-character hex AES-256 master key.",
    "  insert reads the plaintext provider key from stdin only.",
    "  ciphertext uses the AES-256-GCM nonce-prefix framing shared with aesgcm.go.",
    "",
  ].join("\n");
}

function safeCLIErrorMessage(error: unknown, env: Record<string, string | undefined>): string {
  let message = error instanceof PlatformKeyCLIError ? error.message : "platform key operation failed";
  for (const secret of [env.TETRAL_DATABASE_URL, env.ENGINE_VAULT_KEY]) {
    if (secret !== undefined && secret.length > 0) {
      message = message.split(secret).join("[REDACTED]");
    }
  }
  return message;
}

if (import.meta.main) {
  const exitCode = await runPlatformKeyCLI({
    argv: Bun.argv.slice(2),
    stdout: Bun.stdout.writer(),
    stderr: Bun.stderr.writer(),
  });
  process.exit(exitCode);
}
