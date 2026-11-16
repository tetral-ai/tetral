// Layer 2: the Gateway's readiness check, run by Bun as its own process, against
// a real PostgreSQL migrated from the shared test template.
package integration

import (
	"database/sql"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

// The Gateway's readiness check reads the catalog that the Go migrator
// writes. It accepts the migrated catalog under the runtime role, and refuses
// a role that bypasses row security and every drift of the workspace
// isolation contract, naming neither the database nor the role.
func TestGatewayReadinessReadsTheMigratedCatalog(t *testing.T) {
	const (
		roleRefused  = "postgresql runtime role is not permitted"
		driftRefused = "postgresql workspace isolation contract does not match this binary"
	)
	type row struct {
		name    string
		change  func(t *testing.T, runtime, admin *sql.DB)
		refusal string
	}
	rows := []row{
		{name: "migrated catalog", change: func(*testing.T, *sql.DB, *sql.DB) {}},
		{name: "role that bypasses row security", refusal: roleRefused, change: func(t *testing.T, runtime, admin *sql.DB) {
			role := pgx.Identifier{currentRole(t, runtime)}.Sanitize()
			if _, err := admin.Exec("ALTER ROLE " + role + " BYPASSRLS"); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if _, err := admin.Exec("ALTER ROLE " + role + " NOBYPASSRLS"); err != nil {
					t.Error(err)
				}
			})
		}},
	}
	for _, drift := range storagetest.RLSCatalogDrifts(t) {
		rows = append(rows, row{name: drift.Name, refusal: driftRefused, change: func(t *testing.T, runtime, admin *sql.DB) { drift.Apply(t, runtime, admin) }})
	}
	for _, test := range rows {
		t.Run(test.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			test.change(t, runtime, admin)
			databaseURL := storagetest.RuntimeDatabaseURL(t, runtime)
			command := exec.Command("bun", "packages/schema/test/fixtures/live-readiness.ts")
			command.Dir = filepath.Join(repoRootFromBridgeTest(t), "services", "gateway")
			command.Env = append(os.Environ(), "TETRAL_TEST_RUNTIME_DATABASE_URL="+databaseURL)
			output, err := command.CombinedOutput()
			var databaseName string
			if scanErr := runtime.QueryRow(`SELECT current_database()`).Scan(&databaseName); scanErr != nil {
				t.Fatal(scanErr)
			}
			for _, identity := range []string{databaseURL, "postgres://", databaseName, currentRole(t, runtime)} {
				if strings.Contains(string(output), identity) {
					t.Fatal("the readiness check's output names the database or the role")
				}
			}
			switch {
			case test.refusal == "" && err != nil:
				t.Fatalf("readiness refused the migrated catalog: %v: %s", err, output)
			case test.refusal != "" && err == nil:
				t.Fatal("readiness accepted the changed database")
			case test.refusal != "" && !strings.Contains(string(output), test.refusal):
				t.Fatalf("readiness refused with %s, want %q", output, test.refusal)
			}
		})
	}
}

func currentRole(t *testing.T, db *sql.DB) string {
	t.Helper()
	var role string
	if err := db.QueryRow(`SELECT current_user`).Scan(&role); err != nil {
		t.Fatal(err)
	}
	return role
}
