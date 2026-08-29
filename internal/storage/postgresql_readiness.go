package storage

import (
	"context"
	"database/sql"
	"strings"

	"github.com/tetral-ai/tetral/database"
)

type rlsCatalogQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

type catalogPolicy struct {
	table      string
	name       string
	command    string
	using      string
	check      string
	permissive bool
	public     bool
}

func verifyPostgreSQLRLSContract(ctx context.Context, queryer rlsCatalogQueryer) *SchemaMigrationError {
	contract, err := database.LoadPostgreSQL()
	if err != nil {
		return newSchemaMigrationError(SchemaErrorRLSDrift, 1, err)
	}
	expectedWorkspace := stringSet(contract.WorkspaceTables)
	expectedWorkspace[contract.AppendOnlyWorkspaceTable] = true
	expectedGlobal := stringSet(contract.GlobalTables)

	tableRows, err := queryer.QueryContext(ctx, `
		SELECT c.relname,
		       EXISTS (
		         SELECT 1 FROM pg_attribute a
		          WHERE a.attrelid = c.oid AND a.attname = 'workspace_id' AND NOT a.attisdropped
		       ) AS has_workspace_id,
		       c.relrowsecurity,
		       c.relforcerowsecurity
		  FROM pg_class c
		  JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE n.nspname = current_schema()
		   AND c.relkind = 'r'
		 ORDER BY c.relname`)
	if err != nil {
		return newSchemaMigrationError(SchemaErrorRLSDrift, 1, err)
	}
	actualWorkspace := map[string]bool{}
	actualGlobal := map[string]bool{}
	for tableRows.Next() {
		var name string
		var hasWorkspaceID, enabled, forced bool
		if err := tableRows.Scan(&name, &hasWorkspaceID, &enabled, &forced); err != nil {
			_ = tableRows.Close()
			return newSchemaMigrationError(SchemaErrorRLSDrift, 1, err)
		}
		if hasWorkspaceID {
			actualWorkspace[name] = true
			if !enabled || !forced {
				_ = tableRows.Close()
				return newSchemaMigrationError(SchemaErrorRLSDrift, 1, nil)
			}
		} else {
			actualGlobal[name] = true
		}
	}
	if err := tableRows.Close(); err != nil || !equalStringSets(actualWorkspace, expectedWorkspace) || !equalStringSets(actualGlobal, expectedGlobal) {
		return newSchemaMigrationError(SchemaErrorRLSDrift, 1, err)
	}

	policyRows, err := queryer.QueryContext(ctx, `
		SELECT c.relname,
		       p.polname,
		       p.polpermissive,
		       p.polroles = ARRAY[0::oid] AS public_only,
		       CASE p.polcmd WHEN '*' THEN 'ALL' WHEN 'r' THEN 'SELECT' WHEN 'a' THEN 'INSERT' WHEN 'w' THEN 'UPDATE' WHEN 'd' THEN 'DELETE' ELSE '?' END,
		       COALESCE(pg_get_expr(p.polqual, p.polrelid), ''),
		       COALESCE(pg_get_expr(p.polwithcheck, p.polrelid), '')
		  FROM pg_policy p
		  JOIN pg_class c ON c.oid = p.polrelid
		  JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE n.nspname = current_schema()
		 ORDER BY c.relname, p.polname`)
	if err != nil {
		return newSchemaMigrationError(SchemaErrorRLSDrift, 1, err)
	}
	actualPolicies := map[string]catalogPolicy{}
	for policyRows.Next() {
		var policy catalogPolicy
		if err := policyRows.Scan(&policy.table, &policy.name, &policy.permissive, &policy.public, &policy.command, &policy.using, &policy.check); err != nil {
			_ = policyRows.Close()
			return newSchemaMigrationError(SchemaErrorRLSDrift, 1, err)
		}
		actualPolicies[policyKey(policy.table, policy.name)] = policy
	}
	if err := policyRows.Close(); err != nil {
		return newSchemaMigrationError(SchemaErrorRLSDrift, 1, err)
	}
	expectedPolicies := expectedRLSPolicies(contract)
	if len(actualPolicies) != len(expectedPolicies) {
		return newSchemaMigrationError(SchemaErrorRLSDrift, 1, nil)
	}
	for key, expected := range expectedPolicies {
		actual, ok := actualPolicies[key]
		if !ok || !actual.permissive || !actual.public || actual.command != expected.command || normalizePolicyExpression(actual.using) != normalizePolicyExpression(expected.using) || normalizePolicyExpression(actual.check) != normalizePolicyExpression(expected.check) {
			return newSchemaMigrationError(SchemaErrorRLSDrift, 1, nil)
		}
	}
	return nil
}

func expectedRLSPolicies(contract database.PostgreSQL) map[string]catalogPolicy {
	policies := map[string]catalogPolicy{}
	workspaceExpression := "(workspace_id = current_setting('tetral.workspace_id'::text, true))"
	for _, table := range contract.WorkspaceTables {
		policy := catalogPolicy{table: table, name: "workspace_isolation", command: "ALL", using: workspaceExpression, check: workspaceExpression, permissive: true, public: true}
		policies[policyKey(table, policy.name)] = policy
	}
	appendOnly := contract.AppendOnlyWorkspaceTable
	policies[policyKey(appendOnly, "workspace_select")] = catalogPolicy{table: appendOnly, name: "workspace_select", command: "SELECT", using: workspaceExpression, permissive: true, public: true}
	policies[policyKey(appendOnly, "workspace_insert")] = catalogPolicy{table: appendOnly, name: "workspace_insert", command: "INSERT", check: workspaceExpression, permissive: true, public: true}
	for _, declared := range contract.SpecialPolicies {
		policy := catalogPolicy{table: declared.Table, name: declared.Name, command: declared.Command, using: declared.Using, check: declared.Check, permissive: true, public: true}
		policies[policyKey(policy.table, policy.name)] = policy
	}
	return policies
}

func policyKey(table, name string) string { return table + "\x00" + name }

func normalizePolicyExpression(value string) string { return strings.Join(strings.Fields(value), "") }

func stringSet(values []string) map[string]bool {
	set := make(map[string]bool, len(values))
	for _, value := range values {
		set[value] = true
	}
	return set
}

func equalStringSets(left, right map[string]bool) bool {
	if len(left) != len(right) {
		return false
	}
	for value := range left {
		if !right[value] {
			return false
		}
	}
	return true
}
