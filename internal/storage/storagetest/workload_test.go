package storagetest

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/tetral-ai/tetral/database"
)

func TestWorkloadRolesRecoverAfterProcessExit(t *testing.T) {
	if phase := os.Getenv("TETRAL_TEST_WORKLOAD_CRASH_PHASE"); phase != "" {
		_, admin := NewPostgreSQLDBWithAdmin(t)
		if phase == "reserved" || phase == "renamed_role" {
			handleMu.RLock()
			handle := handles[admin]
			handleMu.RUnlock()
			identity, err := randomHex(12)
			if err != nil {
				t.Fatal(err)
			}
			if err := reserveWorkloadRoles(context.Background(), handle, database.RoleDeclarations{Roles: map[string]database.RoleCredential{
				"sandbox": {Name: "tetral_workload_" + identity},
			}}); err != nil {
				t.Fatal(err)
			}
		} else {
			OpenWorkloadDB(t, admin, "sandbox")
		}
		// Bypass all testing cleanups and heartbeat shutdown, as process loss does.
		os.Exit(23)
	}
	for _, phase := range []string{"reserved", "installed", "changed_authority", "renamed_role"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			config, err := parseControlConfig()
			if err != nil {
				t.Fatal(err)
			}
			control := openPool(config)
			defer func() { _ = control.Close() }()
			runID, err := randomHex(16)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := CloseRun(context.Background(), config.ConnString(), runID); err != nil {
					t.Errorf("cleanup interrupted run: %v", err)
				}
			}()
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			command := exec.CommandContext(ctx, executable, "-test.run=^TestWorkloadRolesRecoverAfterProcessExit$")
			command.Env = append(os.Environ(), EnvTestRunID+"="+runID, "TETRAL_TEST_WORKLOAD_CRASH_PHASE="+phase)
			output, err := command.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 23 {
				t.Fatalf("crash fixture: %v; %s", err, output)
			}
			var name string
			if err := control.QueryRowContext(ctx, "SELECT database_name FROM "+cloneRegistryTable+" WHERE run_id=$1", runID).Scan(&name); err != nil {
				t.Fatal(err)
			}
			registration := registrationForDatabase(t, control, name)
			roles, err := registeredWorkloadRoles(ctx, control, registration)
			if err != nil || len(roles) == 0 {
				t.Fatalf("durable role registration: %v, %v", roles, err)
			}
			if phase == "changed_authority" || phase == "renamed_role" {
				role := roles[0]
				//nolint:gosec // PostgreSQL DDL identifiers cannot be bound; pgx quotes the registry-owned name.
				mutateSQL := "COMMENT ON ROLE " + pgx.Identifier{role.Name}.Sanitize() + " IS 'foreign authority'"
				//nolint:gosec // Both the DDL identifier and the authority comment are SQL-quoted.
				restoreSQL := "COMMENT ON ROLE " + pgx.Identifier{role.Name}.Sanitize() + " IS " + quoteLiteral(role.Comment)
				if phase == "renamed_role" {
					// No CONNECT grants exist before installation, so only the role OID
					// can distinguish a renamed role from a role already removed by cleanup.
					renamed := pgx.Identifier{role.Name + "_renamed"}.Sanitize()
					mutateSQL = "ALTER ROLE " + pgx.Identifier{role.Name}.Sanitize() + " RENAME TO " + renamed
					restoreSQL = "ALTER ROLE " + renamed + " RENAME TO " + pgx.Identifier{role.Name}.Sanitize()
				}
				if _, err := control.ExecContext(ctx, mutateSQL); err != nil {
					t.Fatal(err)
				}
				restored := false
				restore := func() {
					if restored {
						return
					}
					if _, err := control.ExecContext(context.Background(), restoreSQL); err != nil {
						t.Error(err)
					} else {
						restored = true
					}
				}
				defer restore()
				if err := CloseRun(ctx, config.ConnString(), runID); err == nil {
					t.Fatal("cleanup accepted changed workload authority")
				}
				var exists bool
				if err := control.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)`, registration.database).Scan(&exists); err != nil || !exists {
					t.Fatalf("rejected cleanup removed clone: %v", err)
				}
				restore()
			}
			if err := CloseRun(ctx, config.ConnString(), runID); err != nil {
				t.Fatalf("recover interrupted workload run: %v", err)
			}
			assertCloneCapabilitiesAbsent(t, control, registration)
			for _, role := range roles {
				var exists bool
				if err := control.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)`, role.Name).Scan(&exists); err != nil || exists {
					t.Fatalf("recovery retained workload role: %t, %v", exists, err)
				}
			}
		})
	}
}
