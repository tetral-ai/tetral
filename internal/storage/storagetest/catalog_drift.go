package storagetest

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/tetral-ai/tetral/database"
)

// CatalogDrift is one change of a migrated catalog that the workspace
// isolation contract forbids.
type CatalogDrift struct {
	Name      string
	statement string
}

const driftRuntimeRole = "{runtime role}"

// RLSCatalogDrifts lists the drifts every readiness check refuses: row
// security switched off or left unforced, a policy broadened, dropped, added
// or bound to the runtime role, and each special policy of the contract
// broadened.
func RLSCatalogDrifts(t testing.TB) []CatalogDrift {
	t.Helper()
	contract, err := database.LoadPostgreSQL()
	if err != nil {
		t.Fatal(err)
	}
	drifts := []CatalogDrift{
		{"rls_disabled", `ALTER TABLE sessions DISABLE ROW LEVEL SECURITY`},
		{"force_disabled", `ALTER TABLE sessions NO FORCE ROW LEVEL SECURITY`},
		{"workspace_expression_broadened", `DROP POLICY workspace_isolation ON sessions; CREATE POLICY workspace_isolation ON sessions USING (true) WITH CHECK (true)`},
		{"append_only_policy_missing", `DROP POLICY workspace_select ON session_file_attachment_consumptions`},
		{"extra_permissive_policy", `CREATE POLICY accidental_access ON sessions USING (true)`},
		{"policy_bound_to_the_runtime_role", `DROP POLICY workspace_isolation ON sessions;
			CREATE POLICY workspace_isolation ON sessions TO ` + driftRuntimeRole + `
			USING (workspace_id = current_setting('tetral.workspace_id', true))
			WITH CHECK (workspace_id = current_setting('tetral.workspace_id', true))`},
	}
	for _, policy := range contract.SpecialPolicies {
		name, table := pgx.Identifier{policy.Name}.Sanitize(), pgx.Identifier{policy.Table}.Sanitize()
		drifts = append(drifts, CatalogDrift{
			Name:      "special_policy_broadened_" + policy.Table + "_" + policy.Name,
			statement: "DROP POLICY " + name + " ON " + table + "; CREATE POLICY " + name + " ON " + table + " FOR ALL USING (true) WITH CHECK (true)",
		})
	}
	return drifts
}

// Apply makes the change in the database of runtime and admin.
func (drift CatalogDrift) Apply(t testing.TB, runtime, admin *sql.DB) {
	t.Helper()
	var role string
	if err := runtime.QueryRow(`SELECT current_user`).Scan(&role); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(strings.ReplaceAll(drift.statement, driftRuntimeRole, pgx.Identifier{role}.Sanitize())); err != nil {
		t.Fatalf("apply %s: %v", drift.Name, err)
	}
}
