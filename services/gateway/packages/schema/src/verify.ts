/**
 * @packageDocumentation
 *
 * Pins the Gateway processes' expected PostgreSQL migration history and
 * verifies the visible migration registry after SQL-client construction but
 * before concrete SQL-backed stores or resolvers are constructed. The
 * provider-gateway and MCP connector startup paths pass this module a tagged
 * SQL client; the verifier asks PostgreSQL for the registry's presence and
 * ordered contents through SELECT statements only. `api` startup is the
 * sole production migration owner and uses Engine storage to apply and stamp
 * migrations.
 *
 * A usable history starts at version one, is contiguous and duplicate-free,
 * has exactly the versions known to this binary, and matches every pinned
 * checksum. Missing, malformed, behind, ahead, gapped, duplicate, and drifted
 * histories fail closed through bounded errors that do not retain SQL driver
 * details or connection material.
 */

/** Pins the checksum expected for PostgreSQL schema migration version one. */
export const PostgreSQLSchemaVersionOneChecksum = "1d0d49961ae132ce19174da7aefd53ee8b396f43e9cfd2ef92460fe0fd5e4b04";

const PostgreSQLSchemaRegistry = [
  PostgreSQLSchemaVersionOneChecksum,
] as const;

/** Enumerates the public-safe failure classifications produced by schema verification. */
export type SchemaVerificationErrorKind =
  | "schema_missing"
  | "schema_behind"
  | "schema_ahead"
  | "schema_history_malformed"
  | "schema_history_gap"
  | "schema_history_duplicate"
  | "schema_checksum_drift";

/** Defines the tagged-template query capability required to read migration history. */
export interface SchemaSQL {
  <T = unknown>(strings: TemplateStringsArray, ...values: unknown[]): PromiseLike<T>;
}

/** Reports a classified schema verification failure without retaining its SQL driver cause. */
export class SchemaVerificationError extends Error {
  readonly kind: SchemaVerificationErrorKind;

  constructor(kind: SchemaVerificationErrorKind) {
    super(publicMessage(kind));
    this.name = "SchemaVerificationError";
    this.kind = kind;
  }
}

interface RegistryPresenceRow {
  readonly exists: boolean;
}

interface AppliedMigrationRow {
  readonly version: number | bigint | string;
  readonly checksum: string;
}

// verifyPostgreSQLSchema is intentionally SELECT-only and needs no migration
// privileges. A caller may still use a broader runtime role for its separately
// authorized credential-update paths.
/**
 * Verifies that PostgreSQL contains the exact ordered migration history
 * expected by this binary.
 *
 * @param sql - Tagged-template query function used for the two read-only
 * migration-registry queries.
 * @throws {@link SchemaVerificationError} When the registry is missing,
 * unreadable, malformed, behind, ahead, non-contiguous, duplicated, or has a
 * checksum that differs from the pinned registry.
 */
export async function verifyPostgreSQLSchema(sql: SchemaSQL): Promise<void> {
  let presence: readonly RegistryPresenceRow[];
  let history: readonly AppliedMigrationRow[];
  try {
    presence = await sql<readonly RegistryPresenceRow[]>`
      SELECT to_regclass('tetral_schema_migrations') IS NOT NULL AS exists
    `;
    if (presence.length !== 1 || typeof presence[0]?.exists !== "boolean") {
      throw new SchemaVerificationError("schema_history_malformed");
    }
    if (!presence[0].exists) {
      throw new SchemaVerificationError("schema_missing");
    }
    history = await sql<readonly AppliedMigrationRow[]>`
      SELECT version, checksum FROM tetral_schema_migrations ORDER BY version
    `;
    if (!Array.isArray(history)) {
      throw new SchemaVerificationError("schema_history_malformed");
    }
  } catch (error) {
    if (error instanceof SchemaVerificationError) throw error;
    throw new SchemaVerificationError("schema_history_malformed");
  }

  const seen = new Set<number>();
  for (const [index, row] of history.entries()) {
    if (row === null || typeof row !== "object") {
      throw new SchemaVerificationError("schema_history_malformed");
    }
    const version = Number(row.version);
    if (!Number.isSafeInteger(version) || version <= 0 || typeof row.checksum !== "string") {
      throw new SchemaVerificationError("schema_history_malformed");
    }
    if (seen.has(version)) {
      throw new SchemaVerificationError("schema_history_duplicate");
    }
    seen.add(version);
    if (version !== index + 1) {
      throw new SchemaVerificationError("schema_history_gap");
    }
    if (index >= PostgreSQLSchemaRegistry.length) {
      throw new SchemaVerificationError("schema_ahead");
    }
    if (row.checksum !== PostgreSQLSchemaRegistry[index]) {
      throw new SchemaVerificationError("schema_checksum_drift");
    }
  }
  if (history.length < PostgreSQLSchemaRegistry.length) {
    throw new SchemaVerificationError("schema_behind");
  }
}

function publicMessage(kind: SchemaVerificationErrorKind): string {
  switch (kind) {
    case "schema_missing": return "postgresql schema registry is missing";
    case "schema_behind": return "postgresql schema is behind this binary";
    case "schema_ahead": return "postgresql schema is ahead of this binary";
    case "schema_checksum_drift": return "postgresql schema checksum does not match this binary";
    default: return "postgresql schema history is malformed";
  }
}
