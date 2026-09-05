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
		{
			table_name: postgresqlContract.append_only_workspace_table,
			has_workspace_id: true,
			rls_enabled: true,
			rls_forced: true,
		},
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
	const appendOnly = postgresqlContract.append_only_workspace_table;
	policies.push(
		{ table_name: appendOnly, policy_name: "workspace_select", permissive: true, public_only: true, command: "SELECT", using_expression: workspaceExpression, check_expression: "" },
		{ table_name: appendOnly, policy_name: "workspace_insert", permissive: true, public_only: true, command: "INSERT", using_expression: "", check_expression: workspaceExpression },
	);
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
		[{ version: 1, checksum: "6f1ec030d986cec0ae83cc9a5abc818045b5d3a388a9434483d05a5bcdd9fc44" }],
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
