import { describe, expect, test } from "bun:test";
import postgresqlContract from "../../../../../../database/postgresql.json";
import {
	SchemaVerificationError,
	verifyPostgreSQLReadiness,
	type SchemaSQL,
} from "../../src/verify.js";

const workspaceExpression = "(workspace_id = current_setting('tetral.workspace_id'::text, true))";

function validTables() {
	return [
		...postgresqlContract.workspace_tables.map((table_name) => ({
			table_name,
			has_workspace_id: true,
			rls_enabled: true,
			rls_forced: true,
		})),
		...postgresqlContract.append_only_workspace_tables.map((table_name) => ({
			table_name,
			has_workspace_id: true,
			rls_enabled: true,
			rls_forced: true,
		})),
		...postgresqlContract.global_tables.map((table_name) => ({
			table_name,
			has_workspace_id: false,
			rls_enabled: false,
			rls_forced: false,
		})),
	];
}

function validPolicies() {
	const policies = postgresqlContract.workspace_tables.map((table_name) => ({
		table_name,
		policy_name: "workspace_isolation",
		permissive: true,
		public_only: true,
		command: "ALL",
		using_expression: workspaceExpression,
		check_expression: workspaceExpression,
	}));
	for (const appendOnly of postgresqlContract.append_only_workspace_tables) {
		policies.push(
			{ table_name: appendOnly, policy_name: "workspace_select", permissive: true, public_only: true, command: "SELECT", using_expression: workspaceExpression, check_expression: "" },
			{ table_name: appendOnly, policy_name: "workspace_insert", permissive: true, public_only: true, command: "INSERT", using_expression: "", check_expression: workspaceExpression },
		);
	}
	for (const policy of postgresqlContract.special_policies) {
		policies.push({
			table_name: policy.table,
			policy_name: policy.name,
			permissive: true,
			public_only: true,
			command: policy.command,
			using_expression: policy.using,
			check_expression: policy.check,
		});
	}
	return policies;
}

function readinessSQL(overrides: { role?: unknown; tables?: unknown; policies?: unknown } = {}): SchemaSQL {
	let query = 0;
	const responses = [
		overrides.role ?? [{ is_superuser: false, bypasses_rls: false }],
		[{ exists: true }],
		[{ version: 1, checksum: "912fd05e7bef3c1b3014a73522f67bae4be93d6759e2a598868dec384427492c" }],
		overrides.tables ?? validTables(),
		overrides.policies ?? validPolicies(),
	];
	return (() => Promise.resolve(responses[query++])) as SchemaSQL;
}

describe("PostgreSQL startup readiness", () => {
	test("accepts the exact runtime role, schema history, and live RLS catalog", async () => {
		await expect(verifyPostgreSQLReadiness(readinessSQL())).resolves.toBeUndefined();
	});

	test("rejects superuser and BYPASSRLS roles without retaining role identity", async () => {
		for (const role of [
			[{ is_superuser: true, bypasses_rls: false }],
			[{ is_superuser: false, bypasses_rls: true }],
		]) {
			await expect(verifyPostgreSQLReadiness(readinessSQL({ role }))).rejects.toMatchObject({
				kind: "runtime_role_invalid",
				message: "postgresql runtime role is not permitted",
			});
		}
	});

	test("names a refusal the server answered by its check, and a failure to reach the database as such", async () => {
		const answered = () => Object.assign(new Error("server refused the query"), { code: "ERR_POSTGRES_SERVER_ERROR", errno: "42501" });
		const unreachable = Object.assign(new Error("Connection closed"), { code: "ERR_POSTGRES_CONNECTION_CLOSED" });
		const checks = ["runtime_role_invalid", "schema_history_malformed", "schema_history_malformed", "schema_rls_drift", "schema_rls_drift"];
		for (const [failing, kind] of checks.entries()) {
			const failingAt = (error: Error): SchemaSQL => {
				const healthy = readinessSQL();
				let query = 0;
				return ((...args: Parameters<SchemaSQL>) => (query++ === failing ? Promise.reject(error) : healthy(...args))) as SchemaSQL;
			};
			await expect(verifyPostgreSQLReadiness(failingAt(answered()))).rejects.toMatchObject({ kind });
			const failure = await verifyPostgreSQLReadiness(failingAt(unreachable)).catch((error: unknown) => error);
			expect(failure).not.toBeInstanceOf(SchemaVerificationError);
			expect(failure).toMatchObject({ message: "postgresql database could not be reached (ERR_POSTGRES_CONNECTION_CLOSED)" });
		}
	});

	test("names a connection the server refused as a database that could not be reached, by its SQLSTATE", async () => {
		// A wrong password or role (28P01, 28000), a missing database (3D000), a
		// server still starting (57P03), a connection fault (08006) and a server
		// out of connections (53300).
		for (const errno of ["28P01", "28000", "3D000", "57P03", "08006", "53300"]) {
			const refused = Object.assign(new Error("server refused the connection"), { code: "ERR_POSTGRES_SERVER_ERROR", errno });
			const failure = await verifyPostgreSQLReadiness((() => Promise.reject(refused)) as SchemaSQL).catch((error: unknown) => error);
			expect(failure, errno).not.toBeInstanceOf(SchemaVerificationError);
			expect(failure, errno).toMatchObject({ message: `postgresql database could not be reached (SQLSTATE ${errno})` });
		}
	});

	test("rejects disabled RLS, policy drift, and unexpected tenant tables", async () => {
		const disabled = validTables();
		disabled[0] = { ...disabled[0]!, rls_forced: false };
		const extraTable = [...validTables(), { table_name: "unexpected", has_workspace_id: true, rls_enabled: true, rls_forced: true }];
		const missingPolicy = validPolicies().slice(1);
		for (const sql of [readinessSQL({ tables: disabled }), readinessSQL({ tables: extraTable }), readinessSQL({ policies: missingPolicy })]) {
			try {
				await verifyPostgreSQLReadiness(sql);
				throw new Error("expected readiness failure");
			} catch (error) {
				expect(error).toBeInstanceOf(SchemaVerificationError);
				expect((error as SchemaVerificationError).kind).toBe("schema_rls_drift");
			}
		}
	});
});
