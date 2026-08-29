package storage_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/tetral-ai/tetral/database"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

func TestVerifySchemaRejectsLiveRLSCatalogDrift(t *testing.T) {
	contract, err := database.LoadPostgreSQL()
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		mutate string
	}{
		{"rls_disabled", `ALTER TABLE sessions DISABLE ROW LEVEL SECURITY`},
		{"force_disabled", `ALTER TABLE sessions NO FORCE ROW LEVEL SECURITY`},
		{"workspace_expression_broadened", `DROP POLICY workspace_isolation ON sessions; CREATE POLICY workspace_isolation ON sessions USING (true) WITH CHECK (true)`},
		{"append_only_policy_missing", `DROP POLICY workspace_select ON session_file_attachment_consumptions`},
		{"extra_permissive_policy", `CREATE POLICY accidental_access ON sessions USING (true)`},
	}
	for _, policy := range contract.SpecialPolicies {
		cases = append(cases, struct {
			name   string
			mutate string
		}{
			name: "special_policy_broadened_" + policy.Table + "_" + policy.Name,
			mutate: "DROP POLICY " + pgx.Identifier{policy.Name}.Sanitize() + " ON " + pgx.Identifier{policy.Table}.Sanitize() +
				"; CREATE POLICY " + pgx.Identifier{policy.Name}.Sanitize() + " ON " + pgx.Identifier{policy.Table}.Sanitize() + " FOR ALL USING (true) WITH CHECK (true)",
		})
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			runtimeDB, adminDB := storagetest.NewPostgreSQLDBWithAdmin(t)
			if err := storage.VerifySchema(context.Background(), runtimeDB); err != nil {
				t.Fatalf("baseline VerifySchema: %v", err)
			}
			assertBunPostgreSQLReadiness(t, runtimeDB, true)
			if _, err := adminDB.Exec(testCase.mutate); err != nil {
				t.Fatalf("mutate catalog: %v", err)
			}
			err := storage.VerifySchema(context.Background(), runtimeDB)
			var schemaErr *storage.SchemaMigrationError
			if !errors.As(err, &schemaErr) || schemaErr.Kind != storage.SchemaErrorRLSDrift {
				t.Fatalf("VerifySchema error = %T %v; want schema_rls_drift", err, err)
			}
			assertBunPostgreSQLReadiness(t, runtimeDB, false)
		})
	}
}

func TestVerifySchemaRejectsNonPublicPolicyTarget(t *testing.T) {
	runtimeDB, adminDB := storagetest.NewPostgreSQLDBWithAdmin(t)
	var role string
	if err := runtimeDB.QueryRow(`SELECT current_user`).Scan(&role); err != nil {
		t.Fatal(err)
	}
	statement := fmt.Sprintf(`DROP POLICY workspace_isolation ON sessions;
		CREATE POLICY workspace_isolation ON sessions TO %s
		USING (workspace_id = current_setting('tetral.workspace_id', true))
		WITH CHECK (workspace_id = current_setting('tetral.workspace_id', true))`, pgx.Identifier{role}.Sanitize())
	if _, err := adminDB.Exec(statement); err != nil {
		t.Fatal(err)
	}
	assertGoRLSDrift(t, runtimeDB)
	assertBunPostgreSQLReadiness(t, runtimeDB, false)
}

func TestVerifySchemaRejectsWorkspaceTableOutsideContract(t *testing.T) {
	db := storagetest.NewPostgreSQLAdminDB(t)
	if _, err := db.Exec(`CREATE TABLE accidental_tenant_data (workspace_id text NOT NULL, id text PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	err := storage.VerifySchema(context.Background(), db)
	var schemaErr *storage.SchemaMigrationError
	if !errors.As(err, &schemaErr) || schemaErr.Kind != storage.SchemaErrorRLSDrift {
		t.Fatalf("VerifySchema error = %T %v; want schema_rls_drift", err, err)
	}
}

func assertGoRLSDrift(t *testing.T, db *sql.DB) {
	t.Helper()
	err := storage.VerifySchema(context.Background(), db)
	var schemaErr *storage.SchemaMigrationError
	if !errors.As(err, &schemaErr) || schemaErr.Kind != storage.SchemaErrorRLSDrift {
		t.Fatalf("VerifySchema error = %T %v; want schema_rls_drift", err, err)
	}
}

func assertBunPostgreSQLReadiness(t *testing.T, db *sql.DB, wantSuccess bool) {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	root := filepath.Dir(filepath.Dir(filepath.Dir(file)))
	dsn := storagetest.RuntimeDatabaseURL(t, db)
	var databaseName, roleName string
	if err := db.QueryRow(`SELECT current_database(), current_user`).Scan(&databaseName, &roleName); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("bun", "packages/schema/test/fixtures/live-readiness.ts")
	command.Dir = filepath.Join(root, "services", "gateway")
	command.Env = append(os.Environ(), "TETRAL_TEST_RUNTIME_DATABASE_URL="+dsn)
	output, err := command.CombinedOutput()
	if wantSuccess && err != nil {
		t.Fatalf("Bun PostgreSQL readiness rejected valid catalog: %v: %s", err, output)
	}
	if !wantSuccess && err == nil {
		t.Fatal("Bun PostgreSQL readiness accepted catalog drift")
	}
	if !wantSuccess && !strings.Contains(string(output), "postgresql workspace isolation contract does not match this binary") {
		t.Fatalf("Bun PostgreSQL readiness returned an unexpected public error: %s", output)
	}
	for _, forbidden := range []string{dsn, "postgres://", databaseName, roleName} {
		if strings.Contains(string(output), forbidden) {
			t.Fatalf("Bun PostgreSQL readiness disclosed database connection identity")
		}
	}
}
