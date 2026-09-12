package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/tetral-ai/tetral/database"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

func TestRunPreparesSchemaAndServingRoles(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(fmt.Sprintf("existing_v1=%t", existing), func(t *testing.T) {
			admin := storagetest.NewEmptyPostgreSQLAdminDB(t)
			declarations := testRoleDeclarations(t)
			defer cleanupInstalledRoles(t, admin, declarations)
			wantVersions := "[1 2 3]"
			var originalStamp time.Time
			if existing {
				// The historical-baseline test owns V1 SQL equivalence. This fixture
				// isolates the command boundary: a V1 catalog already owned by the
				// installed migration role, upgraded through the admin connection.
				if err := storage.MigrateSchema(context.Background(), admin); err != nil {
					t.Fatal(err)
				}
				if _, err := admin.Exec(`ALTER TABLE session_runtime_inbox DROP COLUMN mcp_discovery_attempts, DROP COLUMN mcp_discovery_deadline_at, DROP COLUMN mcp_discovery_diagnostic; DELETE FROM tetral_schema_migrations WHERE version=3; ALTER TABLE session_github_repository_resources DROP COLUMN git_identity_name, DROP COLUMN git_identity_email;
DELETE FROM tetral_schema_migrations WHERE version=2`); err != nil {
					t.Fatal(err)
				}
				conn, err := pgx.Connect(context.Background(), storagetest.AdminDatabaseURL(t, admin))
				if err != nil {
					t.Fatal(err)
				}
				err = database.ApplyRoleContract(context.Background(), conn, declarations)
				_ = conn.Close(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if err := admin.QueryRow(`SELECT applied_at FROM tetral_schema_migrations WHERE version=1`).Scan(&originalStamp); err != nil {
					t.Fatal(err)
				}
				wantVersions = "[2 3]"
			}
			payload, err := json.Marshal(declarations)
			if err != nil {
				t.Fatal(err)
			}
			getenv := func(name string) string {
				if name != adminDatabaseURLEnv {
					return ""
				}
				return storagetest.AdminDatabaseURL(t, admin)
			}
			var logs bytes.Buffer
			if err := run(context.Background(), getenv, bytes.NewReader(payload), &logs); err != nil {
				t.Fatalf("install roles on empty database: %v", err)
			}

			var committed []int
			decoder := json.NewDecoder(&logs)
			for decoder.More() {
				var record map[string]any
				if err := decoder.Decode(&record); err != nil {
					t.Fatal(err)
				}
				if record["msg"] == "schema.migration.completed" {
					if record["transaction.outcome"] != "committed" || record["service.name"] != "db-prepare" {
						t.Fatalf("unexpected migration record: %#v", record)
					}
					committed = append(committed, int(record["schema.version"].(float64)))
				}
			}
			if fmt.Sprint(committed) != wantVersions {
				t.Fatalf("committed versions = %v", committed)
			}
			var tables, stamps int
			if err := admin.QueryRow(`SELECT count(*) FROM information_schema.tables WHERE table_schema='public'`).Scan(&tables); err != nil {
				t.Fatal(err)
			}
			if err := admin.QueryRow(`SELECT count(*) FROM tetral_schema_migrations`).Scan(&stamps); err != nil {
				t.Fatal(err)
			}
			if tables == 0 || stamps != 3 {
				t.Fatalf("installed catalog tables=%d stamps=%d; want nonempty catalog and three stamps", tables, stamps)
			}
			if existing {
				var stamp time.Time
				var checksum string
				if err := admin.QueryRow(`SELECT applied_at, checksum FROM tetral_schema_migrations WHERE version=1`).Scan(&stamp, &checksum); err != nil {
					t.Fatal(err)
				}
				if !stamp.Equal(originalStamp) || checksum != storage.PostgreSQLSchemaVersionOneChecksum {
					t.Fatal("preparation changed V1 history")
				}
			}
			logs.Reset()
			if err := run(context.Background(), getenv, bytes.NewReader(payload), &logs); err != nil {
				t.Fatalf("repeat database preparation: %v", err)
			}
			if strings.Contains(logs.String(), "schema.migration.started") || !strings.Contains(logs.String(), "database.prepare.completed") {
				t.Fatalf("unexpected repeat preparation logs: %s", &logs)
			}
			config, err := pgx.ParseConfig(storagetest.AdminDatabaseURL(t, admin))
			if err != nil {
				t.Fatal(err)
			}
			config.User, config.Password = declarations.Roles["api"].Name, declarations.Roles["api"].Password
			serving := sql.OpenDB(stdlib.GetConnector(*config))
			defer func() { _ = serving.Close() }()
			if err := storage.VerifySchema(context.Background(), serving); err != nil {
				t.Fatalf("serving role cannot verify prepared schema: %v", err)
			}
			if err := storage.VerifyRuntimeRole(context.Background(), serving); err != nil {
				t.Fatalf("prepared API role cannot serve: %v", err)
			}

		})
	}
}

func cleanupInstalledRoles(t *testing.T, admin queryExecer, declarations database.RoleDeclarations) {
	t.Helper()
	var owner string
	if err := admin.QueryRow(`SELECT current_user`).Scan(&owner); err != nil {
		t.Errorf("read test database owner: %v", err)
		return
	}
	migration := declarations.Roles["migration"].Name
	if _, err := admin.Exec("REASSIGN OWNED BY " + pgx.Identifier{migration}.Sanitize() + " TO " + pgx.Identifier{owner}.Sanitize()); err != nil {
		t.Errorf("restore schema ownership: %v", err)
		return
	}
	for _, declaration := range declarations.Roles {
		if _, err := admin.Exec("DROP OWNED BY " + pgx.Identifier{declaration.Name}.Sanitize()); err != nil {
			t.Errorf("remove managed role grants: %v", err)
			return
		}
	}
	control, err := pgx.Connect(context.Background(), os.Getenv(storagetest.EnvTestDatabaseURL))
	if err != nil {
		t.Errorf("connect test control database: %v", err)
		return
	}
	defer func() { _ = control.Close(context.Background()) }()
	for _, declaration := range declarations.Roles {
		if _, err := control.Exec(context.Background(), "DROP ROLE IF EXISTS "+pgx.Identifier{declaration.Name}.Sanitize()); err != nil {
			t.Errorf("drop managed role: %v", err)
			return
		}
	}
}

type queryExecer interface {
	QueryRow(query string, args ...any) *sql.Row
	Exec(query string, args ...any) (sql.Result, error)
}

func TestRunLogsSafeInputFailures(t *testing.T) {
	declarations := testRoleDeclarations(t)
	payload, err := json.Marshal(declarations)
	if err != nil {
		t.Fatal(err)
	}
	incomplete := testRoleDeclarations(t)
	credential := incomplete.Roles["api"]
	credential.Password = ""
	incomplete.Roles["api"] = credential
	incompletePayload, err := json.Marshal(incomplete)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, dsn, input, want string
	}{
		{"missing DSN", "", string(payload), "TETRAL_DATABASE_ADMIN_URL is required"},
		{"malformed JSON", "private-dsn", "{", "must be valid JSON"},
		{"unknown field", "private-dsn", `{"private-input-secret":"private-password"}`, "must be valid JSON"},
		{"trailing value", "private-dsn", string(payload) + " {}", "must contain one JSON value"},
		{"invalid DSN", "postgres://private-user:private-password@private-host:invalid/private-db", string(payload), "connection string is invalid"},
		{"missing credential", "private-dsn", string(incompletePayload), `invalid PostgreSQL role declaration for workload "api"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			var logs bytes.Buffer
			err := run(context.Background(), func(key string) string {
				if key == adminDatabaseURLEnv {
					return test.dsn
				}
				return ""
			}, strings.NewReader(test.input), &logs)
			if err == nil {
				t.Fatal("accepted invalid preparation input")
			}
			assertPreparationFailure(t, logs.String(), "validate_input", test.want)
			for _, private := range []string{"private-", credential.Name, declarations.Roles["api"].Password} {
				if strings.Contains(logs.String(), private) {
					t.Fatal("input leaked into preparation logs")
				}
			}
		})
	}
}

func assertPreparationFailure(t *testing.T, logs, step, message string) {
	t.Helper()
	decoder := json.NewDecoder(strings.NewReader(logs))
	for decoder.More() {
		var record map[string]any
		if err := decoder.Decode(&record); err != nil {
			t.Fatal(err)
		}
		if record["msg"] == "database.prepare.failed" {
			safe, _ := record["error.message_safe"].(string)
			if record["step"] != step || !strings.Contains(safe, message) {
				t.Fatalf("unexpected failure diagnostic: %#v", record)
			}
			return
		}
	}
	t.Fatal("missing preparation failure diagnostic")
}

func testRoleDeclarations(t *testing.T) database.RoleDeclarations {
	t.Helper()
	contract, err := database.LoadRoleContract()
	if err != nil {
		t.Fatal(err)
	}
	declarations := database.RoleDeclarations{Roles: map[string]database.RoleCredential{}}
	for index, workload := range append(contract.WorkloadNames(), contract.MigrationOwner) {
		declarations.Roles[workload] = database.RoleCredential{
			Name:     fmt.Sprintf("tetral_installer_%d_%d", time.Now().UnixNano(), index),
			Password: "installer-test-password-" + workload,
		}
	}
	return declarations
}

func TestRunRejectsInvalidRolesBeforeMigrating(t *testing.T) {
	admin := storagetest.NewEmptyPostgreSQLAdminDB(t)
	var logs bytes.Buffer
	err := run(context.Background(), func(key string) string {
		if key == adminDatabaseURLEnv {
			return storagetest.AdminDatabaseURL(t, admin)
		}
		return ""
	}, bytes.NewBufferString(`{"roles":{}}`), &logs)
	if err == nil {
		t.Fatal("accepted incomplete role declarations")
	}
	assertPreparationFailure(t, logs.String(), "validate_input", "role declarations must exactly match the contract")
	var tables int
	if err := admin.QueryRow(`SELECT count(*) FROM information_schema.tables WHERE table_schema='public'`).Scan(&tables); err != nil {
		t.Fatal(err)
	}
	if tables != 0 {
		t.Fatalf("invalid configuration created %d tables", tables)
	}
}

func TestRunLogsMigrationFailureBeforeInstallingRoles(t *testing.T) {
	admin := storagetest.NewEmptyPostgreSQLAdminDB(t)
	if _, err := admin.Exec(`CREATE FUNCTION reject_migration() RETURNS event_trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'private-command-migration-message' USING ERRCODE = '42501', DETAIL = 'private-command-migration-detail'; END $$;
CREATE EVENT TRIGGER reject_migration ON ddl_command_start WHEN TAG IN ('CREATE TABLE') EXECUTE FUNCTION reject_migration()`); err != nil {
		t.Fatal(err)
	}
	declarations := testRoleDeclarations(t)
	payload, err := json.Marshal(declarations)
	if err != nil {
		t.Fatal(err)
	}
	dsn := storagetest.AdminDatabaseURL(t, admin)
	var logs bytes.Buffer
	err = run(context.Background(), func(key string) string {
		if key == adminDatabaseURLEnv {
			return dsn
		}
		return ""
	}, bytes.NewReader(payload), &logs)
	if err == nil {
		t.Fatal("expected migration failure")
	}
	if strings.Contains(logs.String(), "private-command-migration") || strings.Contains(logs.String(), dsn) {
		t.Fatal("private database detail escaped")
	}
	var failure, summary bool
	decoder := json.NewDecoder(&logs)
	for decoder.More() {
		var record map[string]any
		if err := decoder.Decode(&record); err != nil {
			t.Fatal(err)
		}
		if record["msg"] == "schema.migration.failed" {
			failure = record["service.name"] == "db-prepare" && record["schema.version"] == float64(1) && record["schema.step"] == "create_history" && record["db.sqlstate"] == "42501" && record["transaction.outcome"] == "rolled_back"
		}
		summary = summary || record["msg"] == "database.prepare.failed" && record["step"] == "migrate_schema"
	}
	if !failure || !summary {
		t.Fatalf("migration diagnostic=%t command failure=%t", failure, summary)
	}
	var installed bool
	if err := admin.QueryRow(`SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=$1)`, declarations.Roles["migration"].Name).Scan(&installed); err != nil {
		t.Fatal(err)
	}
	if installed {
		t.Fatal("installed roles after migration failure")
	}
}

func TestRunRejectsNonSuperuserBeforeMigrating(t *testing.T) {
	admin := storagetest.NewEmptyPostgreSQLAdminDB(t)
	role := fmt.Sprintf("tetral_prepare_admin_%d", time.Now().UnixNano())
	quotedRole := pgx.Identifier{role}.Sanitize()
	if _, err := admin.Exec("CREATE ROLE " + quotedRole + " LOGIN NOSUPERUSER CREATEROLE PASSWORD 'private-admin-password'"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := admin.Exec("DROP OWNED BY " + quotedRole); err != nil {
			t.Error(err)
		}
		if _, err := admin.Exec("DROP ROLE " + quotedRole); err != nil {
			t.Error(err)
		}
	}()
	// The private test database revokes PUBLIC CONNECT. Give this account
	// connectivity so the command reaches the superuser prerequisite check.
	var databaseName string
	if err := admin.QueryRow("SELECT current_database()").Scan(&databaseName); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec("GRANT CONNECT ON DATABASE " + pgx.Identifier{databaseName}.Sanitize() + " TO " + quotedRole); err != nil {
		t.Fatal(err)
	}
	dsn, err := url.Parse(storagetest.AdminDatabaseURL(t, admin))
	if err != nil {
		t.Fatal(err)
	}
	dsn.User = url.UserPassword(role, "private-admin-password")
	payload, err := json.Marshal(testRoleDeclarations(t))
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	err = run(context.Background(), func(key string) string {
		if key == adminDatabaseURLEnv {
			return dsn.String()
		}
		return ""
	}, bytes.NewReader(payload), &logs)
	if err == nil {
		t.Fatal("accepted CREATEROLE administrator without superuser")
	}
	assertPreparationFailure(t, logs.String(), "verify_admin", "requires a superuser connection")
	if strings.Contains(logs.String(), role) || strings.Contains(logs.String(), "private-admin-password") {
		t.Fatal("administrative credentials escaped")
	}
	var tables int
	if err := admin.QueryRow(`SELECT count(*) FROM information_schema.tables WHERE table_schema='public'`).Scan(&tables); err != nil {
		t.Fatal(err)
	}
	if tables != 0 {
		t.Fatalf("privilege preflight created %d tables", tables)
	}
}

func TestRunLogsRoleConflictAfterCommittedMigration(t *testing.T) {
	admin := storagetest.NewEmptyPostgreSQLAdminDB(t)
	declarations := testRoleDeclarations(t)
	conflict := declarations.Roles["api"]
	quotedRole := pgx.Identifier{conflict.Name}.Sanitize()
	if _, err := admin.Exec("CREATE ROLE " + quotedRole + " NOLOGIN NOINHERIT"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := admin.Exec("DROP ROLE " + quotedRole); err != nil {
			t.Error(err)
		}
	}()
	payload, err := json.Marshal(declarations)
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	err = run(context.Background(), func(key string) string {
		if key == adminDatabaseURLEnv {
			return storagetest.AdminDatabaseURL(t, admin)
		}
		return ""
	}, bytes.NewReader(payload), &logs)
	if err == nil {
		t.Fatal("accepted an unmanaged existing role")
	}
	assertPreparationFailure(t, logs.String(), "apply_roles", "role declaration conflicts with an existing role")
	if strings.Contains(logs.String(), conflict.Name) || strings.Contains(logs.String(), conflict.Password) {
		t.Fatal("role credentials escaped")
	}
	var versions int
	if err := admin.QueryRow("SELECT count(*) FROM tetral_schema_migrations").Scan(&versions); err != nil {
		t.Fatal(err)
	}
	if versions != 3 {
		t.Fatalf("role failure changed migration history: %d versions", versions)
	}
}
