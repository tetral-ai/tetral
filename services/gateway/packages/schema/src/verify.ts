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

import postgresqlContractJSON from "../../../../../database/postgresql.json";

/** Pins the checksum expected for PostgreSQL schema migration version one. */
export const PostgreSQLSchemaVersionOneChecksum =
	"6f1ec030d986cec0ae83cc9a5abc818045b5d3a388a9434483d05a5bcdd9fc44";

const PostgreSQLSchemaRegistry = [PostgreSQLSchemaVersionOneChecksum] as const;

/** Enumerates the public-safe failure classifications produced by schema verification. */
export type SchemaVerificationErrorKind =
	| "schema_missing"
	| "schema_behind"
	| "schema_ahead"
	| "schema_history_malformed"
	| "schema_history_gap"
	| "schema_history_duplicate"
	| "schema_checksum_drift"
	| "schema_rls_drift"
	| "runtime_role_invalid";

/** Defines the tagged-template query capability required to read migration history. */
export type SchemaSQL = <T = unknown>(
	strings: TemplateStringsArray,
	...values: unknown[]
) => PromiseLike<T>;

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

interface RuntimeRoleRow {
	readonly is_superuser: boolean;
	readonly bypasses_rls: boolean;
}

interface CatalogTableRow {
	readonly table_name: string;
	readonly has_workspace_id: boolean;
	readonly rls_enabled: boolean;
	readonly rls_forced: boolean;
}

interface CatalogPolicyRow {
	readonly table_name: string;
	readonly policy_name: string;
	readonly permissive: boolean;
	readonly public_only: boolean;
	readonly command: string;
	readonly using_expression: string;
	readonly check_expression: string;
}

interface PostgreSQLContract {
	readonly version: number;
	readonly workspace_tables: readonly string[];
	readonly append_only_workspace_table: string;
	readonly global_tables: readonly string[];
	readonly special_policies: readonly {
		readonly table: string;
		readonly name: string;
		readonly command: string;
		readonly using: string;
		readonly check: string;
	}[];
}

const postgresqlContract = postgresqlContractJSON as PostgreSQLContract;

/** Verifies runtime-role safety, migration history, and the live RLS catalog. */
export async function verifyPostgreSQLReadiness(sql: SchemaSQL): Promise<void> {
	await verifyRuntimeRole(sql);
	await verifyPostgreSQLSchema(sql);
	await verifyPostgreSQLRLS(sql);
}

async function verifyRuntimeRole(sql: SchemaSQL): Promise<void> {
	let rows: readonly RuntimeRoleRow[];
	try {
		rows = await sql<readonly RuntimeRoleRow[]>`
      SELECT rolsuper AS is_superuser, rolbypassrls AS bypasses_rls
        FROM pg_roles WHERE rolname = current_user
    `;
	} catch {
		throw new SchemaVerificationError("runtime_role_invalid");
	}
	const row = rows[0];
	if (rows.length !== 1 || row === undefined || row.is_superuser || row.bypasses_rls) {
		throw new SchemaVerificationError("runtime_role_invalid");
	}
}

async function verifyPostgreSQLRLS(sql: SchemaSQL): Promise<void> {
	let tables: readonly CatalogTableRow[];
	let policies: readonly CatalogPolicyRow[];
	try {
		tables = await sql<readonly CatalogTableRow[]>`
      SELECT c.relname AS table_name,
             EXISTS (
               SELECT 1 FROM pg_attribute a
                WHERE a.attrelid = c.oid AND a.attname = 'workspace_id' AND NOT a.attisdropped
             ) AS has_workspace_id,
             c.relrowsecurity AS rls_enabled,
             c.relforcerowsecurity AS rls_forced
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE n.nspname = current_schema() AND c.relkind = 'r'
       ORDER BY c.relname
    `;
		policies = await sql<readonly CatalogPolicyRow[]>`
      SELECT c.relname AS table_name,
             p.polname AS policy_name,
             p.polpermissive AS permissive,
             p.polroles = ARRAY[0::oid] AS public_only,
             CASE p.polcmd WHEN '*' THEN 'ALL' WHEN 'r' THEN 'SELECT' WHEN 'a' THEN 'INSERT' WHEN 'w' THEN 'UPDATE' WHEN 'd' THEN 'DELETE' ELSE '?' END AS command,
             COALESCE(pg_get_expr(p.polqual, p.polrelid), '') AS using_expression,
             COALESCE(pg_get_expr(p.polwithcheck, p.polrelid), '') AS check_expression
        FROM pg_policy p
        JOIN pg_class c ON c.oid = p.polrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE n.nspname = current_schema()
       ORDER BY c.relname, p.polname
    `;
	} catch {
		throw new SchemaVerificationError("schema_rls_drift");
	}
	if (!catalogTablesMatch(tables) || !catalogPoliciesMatch(policies)) {
		throw new SchemaVerificationError("schema_rls_drift");
	}
}

function catalogTablesMatch(rows: readonly CatalogTableRow[]): boolean {
	const expectedWorkspace = new Set([
		...postgresqlContract.workspace_tables,
		postgresqlContract.append_only_workspace_table,
	]);
	const expectedGlobal = new Set(postgresqlContract.global_tables);
	const actualWorkspace = new Set<string>();
	const actualGlobal = new Set<string>();
	for (const row of rows) {
		if (row.has_workspace_id) {
			if (!row.rls_enabled || !row.rls_forced) return false;
			actualWorkspace.add(row.table_name);
		} else {
			actualGlobal.add(row.table_name);
		}
	}
	return equalSets(actualWorkspace, expectedWorkspace) && equalSets(actualGlobal, expectedGlobal);
}

function catalogPoliciesMatch(rows: readonly CatalogPolicyRow[]): boolean {
	const expected = expectedPolicies();
	if (rows.length !== expected.size) return false;
	for (const row of rows) {
		const policy = expected.get(`${row.table_name}\0${row.policy_name}`);
		if (
			policy === undefined ||
			!row.permissive ||
			!row.public_only ||
			row.command !== policy.command ||
			normalizeExpression(row.using_expression) !== normalizeExpression(policy.using) ||
			normalizeExpression(row.check_expression) !== normalizeExpression(policy.check)
		) return false;
	}
	return true;
}

function expectedPolicies(): Map<string, { command: string; using: string; check: string }> {
	const result = new Map<string, { command: string; using: string; check: string }>();
	const workspace = "(workspace_id = current_setting('tetral.workspace_id'::text, true))";
	for (const table of postgresqlContract.workspace_tables) {
		result.set(`${table}\0workspace_isolation`, { command: "ALL", using: workspace, check: workspace });
	}
	const appendOnly = postgresqlContract.append_only_workspace_table;
	result.set(`${appendOnly}\0workspace_select`, { command: "SELECT", using: workspace, check: "" });
	result.set(`${appendOnly}\0workspace_insert`, { command: "INSERT", using: "", check: workspace });
	for (const policy of postgresqlContract.special_policies) {
		result.set(`${policy.table}\0${policy.name}`, policy);
	}
	return result;
}

function equalSets(left: ReadonlySet<string>, right: ReadonlySet<string>): boolean {
	if (left.size !== right.size) return false;
	for (const value of left) if (!right.has(value)) return false;
	return true;
}

function normalizeExpression(value: string): string {
	return value.replaceAll(/\s+/g, "");
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
		if (
			!Number.isSafeInteger(version) ||
			version <= 0 ||
			typeof row.checksum !== "string"
		) {
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
		case "schema_missing":
			return "postgresql schema registry is missing";
		case "schema_behind":
			return "postgresql schema is behind this binary";
		case "schema_ahead":
			return "postgresql schema is ahead of this binary";
		case "schema_checksum_drift":
			return "postgresql schema checksum does not match this binary";
		case "schema_rls_drift":
			return "postgresql workspace isolation contract does not match this binary";
		case "runtime_role_invalid":
			return "postgresql runtime role is not permitted";
		default:
			return "postgresql schema history is malformed";
	}
}
