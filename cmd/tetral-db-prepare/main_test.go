package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/tetral-ai/tetral/database"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/storage/catalogtest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

func TestRunPreparesSchemaAndServingRoles(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(fmt.Sprintf("matching_current=%t", existing), func(t *testing.T) {
			admin := storagetest.NewEmptyPostgreSQLAdminDB(t)
			declarations := testRoleDeclarations(t)
			defer cleanupInstalledRoles(t, admin, declarations)
			wantVersions := "[1]"
			var originalStamp time.Time
			if existing {
				// Repeating the matching initial schema preserves its stamp and
				// catalog ownership; predecessor schemas are rejected separately.
				if err := storage.MigrateSchema(context.Background(), admin); err != nil {
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
				wantVersions = "[]"
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
			if err := runLocalPreparationFixture(context.Background(), getenv, bytes.NewReader(payload), &logs); err != nil {
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
			if tables == 0 || stamps != 1 {
				t.Fatalf("installed catalog tables=%d stamps=%d; want nonempty catalog and one current stamp", tables, stamps)
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
			if err := runLocalPreparationFixture(context.Background(), getenv, bytes.NewReader(payload), &logs); err != nil {
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
	err := runLocalPreparationFixture(context.Background(), func(key string) string {
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
	if _, err := admin.Exec(`CREATE FUNCTION pg_temp.reject_migration() RETURNS event_trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'private-command-migration-message' USING ERRCODE = '42501', DETAIL = 'private-command-migration-detail'; END $$;
CREATE EVENT TRIGGER reject_migration ON ddl_command_start WHEN TAG IN ('CREATE TABLE') EXECUTE FUNCTION pg_temp.reject_migration()`); err != nil {
		t.Fatal(err)
	}
	declarations := testRoleDeclarations(t)
	payload, err := json.Marshal(declarations)
	if err != nil {
		t.Fatal(err)
	}
	dsn := storagetest.AdminDatabaseURL(t, admin)
	var logs bytes.Buffer
	err = runLocalPreparationFixture(context.Background(), func(key string) string {
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
	err = runLocalPreparationFixture(context.Background(), func(key string) string {
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
	err = runLocalPreparationFixture(context.Background(), func(key string) string {
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
	if versions != 1 {
		t.Fatalf("role failure changed migration history: %d versions", versions)
	}
}

// Schema and role transaction tests use their isolated plaintext PostgreSQL
// fixture through an explicit connector. Production run always constructs the
// verified TLS owner; protected-store integration exercises that real transport.
func runLocalPreparationFixture(ctx context.Context, getenv func(string) string, input io.Reader, stderr io.Writer) error {
	return runWithConnection(ctx, getenv, input, stderr, func(_ context.Context, getenv func(string) string) (preparationConnection, error) {
		config, err := pgx.ParseConfig(getenv(adminDatabaseURLEnv))
		return preparationConnection{config: config, refresh: func(context.Context, *pgx.ConnConfig) error { return nil }, close: func() error { return nil }}, err
	})
}

func TestRunRequiresProtectedDatabaseTransportBeforeConnecting(t *testing.T) {
	payload, err := json.Marshal(testRoleDeclarations(t))
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	err = run(context.Background(), func(key string) string {
		if key == adminDatabaseURLEnv {
			return "postgres://private-user:private-password@127.0.0.1:1/private-db?sslmode=disable"
		}
		return ""
	}, bytes.NewReader(payload), &logs)
	if err == nil {
		t.Fatal("accepted missing database trust")
	}
	assertPreparationFailure(t, logs.String(), "configure_transport", "requires valid CA trust and server name")
	if strings.Contains(logs.String(), "private-") {
		t.Fatal("database connection details escaped")
	}
}

func TestRunRejectsPredecessorWithoutChangingCatalogOrRoles(t *testing.T) {
	admin := storagetest.NewEmptyPostgreSQLAdminDB(t)
	// This stamp identifies the retired initial schema. It is a rejection
	// fixture, never an initializer or compatibility implementation.
	if _, err := admin.Exec(`CREATE TABLE tetral_schema_migrations (version BIGINT PRIMARY KEY, checksum TEXT NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP);
INSERT INTO tetral_schema_migrations (version,checksum) VALUES (1,'d42f4f8936525f02525b621e943d9ad98a91c6d8a76ca11a309c62dee496ade6');
CREATE TABLE predecessor_data (value TEXT NOT NULL); INSERT INTO predecessor_data VALUES ('preserve-me')`); err != nil {
		t.Fatal(err)
	}
	before, err := catalogtest.Snapshot(context.Background(), admin)
	if err != nil {
		t.Fatal(err)
	}
	declarations := testRoleDeclarations(t)
	payload, err := json.Marshal(declarations)
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	err = runLocalPreparationFixture(context.Background(), func(key string) string {
		if key == adminDatabaseURLEnv {
			return storagetest.AdminDatabaseURL(t, admin)
		}
		return ""
	}, bytes.NewReader(payload), &logs)
	if err == nil {
		t.Fatal("predecessor schema was accepted")
	}
	after, err := catalogtest.Snapshot(context.Background(), admin)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("rejected predecessor catalog changed")
	}
	var value string
	if err := admin.QueryRow(`SELECT value FROM predecessor_data`).Scan(&value); err != nil || value != "preserve-me" {
		t.Fatalf("predecessor data changed: %q %v", value, err)
	}
	for _, role := range declarations.Roles {
		var exists bool
		if err := admin.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)`, role.Name).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists {
			t.Fatal("predecessor rejection installed a role")
		}
	}
}

func TestRunRejectsUnregisteredNamespaceObjectsWithoutMutationOrRoles(t *testing.T) {
	for _, test := range []struct{ name, setup string }{
		{"enum", `CREATE TYPE private_unregistered_enum AS ENUM ('first','second')`},
		{"domain", `CREATE DOMAIN private_unregistered_domain AS text DEFAULT 'private-domain-default' NOT NULL CHECK (VALUE <> '')`},
		{"collation", `CREATE COLLATION private_unregistered_collation (provider=libc, locale='C')`},
	} {
		t.Run(test.name, func(t *testing.T) {
			admin := storagetest.NewEmptyPostgreSQLAdminDB(t)
			if _, err := admin.Exec(test.setup); err != nil {
				t.Fatal(err)
			}
			before := preparationNamespaceCatalog(t, admin)
			declarations := testRoleDeclarations(t)
			// A broken guard can install roles before this test reports failure.
			// Reclaim that failed-case ownership without hiding its evidence.
			t.Cleanup(func() {
				var installed bool
				if err := admin.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)`, declarations.Roles["migration"].Name).Scan(&installed); err != nil {
					t.Error(err)
					return
				}
				if installed {
					cleanupInstalledRoles(t, admin, declarations)
				}
			})
			payload, err := json.Marshal(declarations)
			if err != nil {
				t.Fatal(err)
			}
			dsn := storagetest.AdminDatabaseURL(t, admin)
			var logs bytes.Buffer
			err = runLocalPreparationFixture(context.Background(), func(key string) string {
				if key == adminDatabaseURLEnv {
					return dsn
				}
				return ""
			}, bytes.NewReader(payload), &logs)
			if err == nil {
				t.Fatal("preparation accepted an unregistered namespace object")
			}
			if after := preparationNamespaceCatalog(t, admin); after != before {
				t.Fatal("rejected preparation changed namespace or object catalog metadata")
			}
			var sessions, history bool
			if err := admin.QueryRow(`SELECT to_regclass('sessions') IS NOT NULL,to_regclass('tetral_schema_migrations') IS NOT NULL`).Scan(&sessions, &history); err != nil {
				t.Fatal(err)
			}
			if sessions || history {
				t.Fatal("rejected preparation initialized canonical objects or identity")
			}
			for _, role := range declarations.Roles {
				var exists bool
				if err := admin.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)`, role.Name).Scan(&exists); err != nil {
					t.Fatal(err)
				}
				if exists {
					t.Fatal("unregistered object rejection installed a role")
				}
			}
			output := logs.String()
			for _, private := range []string{dsn, "private_unregistered", "private-domain-default", "CREATE TYPE", "CREATE DOMAIN", "CREATE COLLATION"} {
				if strings.Contains(output, private) {
					t.Fatal("private object or connection detail escaped preparation diagnostics")
				}
			}
			for _, role := range declarations.Roles {
				if strings.Contains(output, role.Password) {
					t.Fatal("role credential escaped preparation diagnostics")
				}
			}
			var schemaFailures, commandFailures int
			decoder := json.NewDecoder(strings.NewReader(output))
			for {
				var record map[string]any
				if err := decoder.Decode(&record); err == io.EOF {
					break
				} else if err != nil {
					t.Fatal(err)
				}
				switch record["msg"] {
				case "schema.migration.started", "schema.migration.completed", "database.prepare.completed":
					t.Fatal("rejected initialization reported transaction or command success")
				case "schema.migration.failed":
					if record["service.name"] != "db-prepare" || record["schema.step"] != "verify_empty_schema" || record["transaction.outcome"] != "not_started" || record["error.code"] != string(storage.SchemaErrorUnexpectedState) || record["schema.version"] != nil {
						t.Fatalf("unexpected pre-mutation schema failure diagnostic: %v", record)
					}
					schemaFailures++
				case "database.prepare.failed":
					if record["step"] != "migrate_schema" || schemaFailures != 1 {
						t.Fatalf("command failure did not follow namespace rejection: %v", record)
					}
					commandFailures++
				}
			}
			if schemaFailures != 1 || commandFailures != 1 {
				t.Fatalf("schema/command failure counts=%d/%d; want1/1", schemaFailures, commandFailures)
			}
		})
	}
}

// Inspect the same private namespace before and after the actual command,
// including exact type definitions/labels, constraints, ownership and OIDs.
// There is no canonical history table to query in these rejected fixtures.
func preparationNamespaceCatalog(t *testing.T, db *sql.DB) string {
	t.Helper()
	var snapshot string
	if err := db.QueryRow(`SELECT jsonb_build_object(
		'namespace',to_jsonb(n),
		'dependencies',(SELECT jsonb_agg(to_jsonb(d) ORDER BY d.classid,d.objid,d.objsubid,d.refclassid,d.refobjid,d.refobjsubid,d.deptype)
			FROM pg_catalog.pg_depend d WHERE d.refclassid='pg_catalog.pg_namespace'::pg_catalog.regclass AND d.refobjid=n.oid),
		'types',(SELECT jsonb_agg(to_jsonb(t) ORDER BY t.oid) FROM pg_catalog.pg_type t WHERE t.typnamespace=n.oid),
		'enum_values',(SELECT jsonb_agg(to_jsonb(e) ORDER BY e.enumtypid,e.enumsortorder)
			FROM pg_catalog.pg_enum e JOIN pg_catalog.pg_type t ON t.oid=e.enumtypid WHERE t.typnamespace=n.oid),
		'constraints',(SELECT jsonb_agg(to_jsonb(c) ORDER BY c.oid) FROM pg_catalog.pg_constraint c WHERE c.connamespace=n.oid),
		'collations',(SELECT jsonb_agg(to_jsonb(c) ORDER BY c.oid) FROM pg_catalog.pg_collation c WHERE c.collnamespace=n.oid),
		'relations',(SELECT jsonb_agg(to_jsonb(c) ORDER BY c.oid) FROM pg_catalog.pg_class c WHERE c.relnamespace=n.oid),
		'routines',(SELECT jsonb_agg(to_jsonb(p) ORDER BY p.oid) FROM pg_catalog.pg_proc p WHERE p.pronamespace=n.oid)
	)::text FROM pg_catalog.pg_namespace n WHERE n.nspname=pg_catalog.current_schema()`).Scan(&snapshot); err != nil {
		t.Fatalf("snapshot preparation namespace catalog: %v", err)
	}
	return snapshot
}
