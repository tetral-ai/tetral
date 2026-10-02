/** Process-owned Bun PostgreSQL pool policy. Go pools have a distinct owner and policy. */
export { asSQLSource, fixedSQLSource, openPostgresSQLOwner } from "./owner.js";
export type { SQLSource, PostgresSQLOwner, PostgresSQLOwnerOptions, SQLOwnerObservation } from "./owner.js";
export interface DatabasePoolConfig {
  readonly max: number;
  /** Idle connection timeout, seconds. */
  readonly idleTimeout: number;
  /** Maximum connection lifetime, seconds. */
  readonly maxLifetime: number;
  /** Connection establishment timeout, seconds. */
  readonly connectionTimeout: number;
  /** PostgreSQL statement timeout, milliseconds. */
  readonly statementTimeoutMs: number;
}

/** All bounds are positive safe integers, read and validated once at startup. */
export const databasePoolDefaults: Readonly<DatabasePoolConfig> = Object.freeze({
  max: 10,
  idleTimeout: 30,
  maxLifetime: 1_800,
  connectionTimeout: 30,
  statementTimeoutMs: 30_000,
});

/** Parse recognized pool keys; unrelated environment belongs to the process owner. */
export function parseDatabasePoolConfig(
  env: Readonly<Record<string, string | undefined>>,
  policy: { readonly empty: "default" | "reject" },
): DatabasePoolConfig | undefined {
  const parse = (key: string, fallback: number): number | undefined => {
    const value = env[key];
    if (value === undefined || (value === "" && policy.empty === "default")) return fallback;
    if (!/^[1-9][0-9]*$/.test(value)) return undefined;
    const parsed = Number(value);
    return Number.isSafeInteger(parsed) ? parsed : undefined;
  };
  const max = parse("TETRAL_DATABASE_POOL_MAX", databasePoolDefaults.max);
  const idleTimeout = parse("TETRAL_DATABASE_POOL_IDLE_TIMEOUT_SECONDS", databasePoolDefaults.idleTimeout);
  const maxLifetime = parse("TETRAL_DATABASE_POOL_MAX_LIFETIME_SECONDS", databasePoolDefaults.maxLifetime);
  const connectionTimeout = parse("TETRAL_DATABASE_POOL_CONNECTION_TIMEOUT_SECONDS", databasePoolDefaults.connectionTimeout);
  const statementTimeoutMs = parse("TETRAL_DATABASE_STATEMENT_TIMEOUT_MS", databasePoolDefaults.statementTimeoutMs);
  if (max === undefined || idleTimeout === undefined || maxLifetime === undefined || connectionTimeout === undefined || statementTimeoutMs === undefined) return undefined;
  return Object.freeze({ max, idleTimeout, maxLifetime, connectionTimeout, statementTimeoutMs });
}
